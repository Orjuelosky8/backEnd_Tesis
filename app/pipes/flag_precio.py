# app/pipes/flag_precio.py
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import numpy as np
from sqlalchemy import text, select
from sqlalchemy.orm import Session

from db.schema import Licitacion
from db import repo


# ============================================================
# Config / Parámetros por defecto
# ============================================================

TOP_K = 50
MIN_NEIGHBORS_FOR_STATS = 10
Z_MAD_THRESHOLD = 2.8

MAX_TARGET_CHUNKS = 128
MAX_CANDIDATES = 5000
MAX_CAND_PER_LIC_CHUNKS = 64

STRICT_FILTER_MODALIDAD = True
STRICT_FILTER_ACT_ECON = True
PENALTY_ESTADO = 0.10  # penalización si cambia 'estado'


# ============================================================
# Helpers / utilitarios
# ============================================================

def _ensure_flag(session: Session) -> None:
    """Asegura que el flag exista en public.flags (idempotente)."""
    session.execute(text("""
        INSERT INTO public.flags (codigo, nombre, descripcion)
        VALUES (
          'red_precio',
          'Desviación de precio (comparables)',
          'Evalúa si la cuantía está fuera del rango robusto (IQR/zMAD) de comparables similares.'
        )
        ON CONFLICT (codigo) DO UPDATE
          SET nombre = EXCLUDED.nombre,
              descripcion = EXCLUDED.descripcion
    """))
    session.commit()


def _to_np_vec(v) -> np.ndarray | None:
    """
    Convierte lo que llega de pgvector (list, memoryview, np.ndarray, etc.)
    a np.ndarray(float32) 1D. Devuelve None si no es usable.
    """
    if v is None:
        return None
    try:
        if isinstance(v, np.ndarray):
            arr = v.astype(np.float32, copy=False)
        elif isinstance(v, (list, tuple)):
            arr = np.asarray(v, dtype=np.float32)
        else:
            # memoryview / bytes / otros tipos: intenta convertir
            arr = np.asarray(v, dtype=np.float32)
        if arr.ndim == 1 and arr.size > 0 and np.isfinite(arr).all():
            return arr
    except Exception:
        return None
    return None


def _l2_normalize(x: np.ndarray) -> np.ndarray:
    n = float(np.linalg.norm(x))
    if not np.isfinite(n) or n == 0.0:
        return x.astype(np.float32, copy=False)
    return (x / n).astype(np.float32, copy=False)


def _fmt_money(x: Optional[float]) -> str:
    """Formatea dinero como $ 1.234.567 (espaciado/estilo rápido)."""
    if x is None or not np.isfinite(x):
        return "—"
    return f"${x:,.0f}".replace(",", ".")


# ============================================================
# Estadística robusta
# ============================================================

@dataclass
class RobustStats:
    median: float
    mad: float
    z_mad: float
    q1: float
    q3: float
    iqr: float
    lower: float
    upper: float
    n: int


def _robust_stats(values: np.ndarray, target: float) -> RobustStats:
    arr = np.asarray(values, dtype=float)
    med = float(np.median(arr)) if arr.size else 0.0
    abs_dev = np.abs(arr - med) if arr.size else np.array([0.0])
    mad = float(np.median(abs_dev))
    z_mad = 0.0 if mad == 0 or not np.isfinite(target) else float(0.6745 * (target - med) / mad)
    q1 = float(np.percentile(arr, 25)) if arr.size else 0.0
    q3 = float(np.percentile(arr, 75)) if arr.size else 0.0
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return RobustStats(med, mad, z_mad, q1, q3, iqr, lower, upper, int(arr.size))


# ============================================================
# Fetch en SQL (sin migraciones)
# ============================================================

def _fetch_target_docvec(session: Session, lic_id: int) -> Optional[np.ndarray]:
    """
    DocVec = promedio normalizado de los embeddings por chunk (cosine pooling).
    """
    rows = session.execute(text("""
        SELECT embedding_vec
        FROM public.licitacion_chunk
        WHERE licitacion_id = :id AND embedding_vec IS NOT NULL
        ORDER BY id
        LIMIT :m
    """), {"id": lic_id, "m": MAX_TARGET_CHUNKS}).fetchall()

    vecs = []
    for (v,) in rows:
        nv = _to_np_vec(v)
        if nv is not None:
            vecs.append(_l2_normalize(nv))

    if not vecs:
        return None

    mean = np.vstack(vecs).mean(axis=0)
    return _l2_normalize(mean)


def _fetch_target_meta(session: Session, lic_id: int) -> Dict:
    row = session.execute(text("""
        SELECT modalidad, act_econ, estado, cuantia
        FROM public.licitacion WHERE id = :id
    """), {"id": lic_id}).fetchone()
    if not row:
        return {}
    return {
        "modalidad": row[0],
        "act_econ": row[1],
        "estado": row[2],
        "cuantia": float(row[3]) if row[3] is not None else None,
    }


def _fetch_candidate_headers(session: Session, lic_id: int, filt: Dict) -> List[Tuple[int, Optional[str], Optional[str], Optional[str], Optional[float]]]:
    """
    Devuelve (id, modalidad, act_econ, estado, cuantia) de candidatas,
    con filtro fuerte por modalidad/act_econ si se configuró.
    """
    where = ["id <> :id"]
    params = {"id": lic_id, "lim": MAX_CANDIDATES}
    if STRICT_FILTER_MODALIDAD and filt.get("modalidad"):
        where.append("modalidad = :mod")
        params["mod"] = filt["modalidad"]
    if STRICT_FILTER_ACT_ECON and filt.get("act_econ"):
        where.append("act_econ = :act")
        params["act"] = filt["act_econ"]

    sql = f"""
        SELECT id, modalidad, act_econ, estado, cuantia
        FROM public.licitacion
        WHERE {" AND ".join(where)}
        ORDER BY id
        LIMIT :lim
    """
    return session.execute(text(sql), params).fetchall()


def _fetch_candidate_chunks_docvecs(session: Session, cand_ids: List[int]) -> Dict[int, np.ndarray]:
    if not cand_ids:
        return {}

    rows = session.execute(text("""
        SELECT lic.licitacion_id,
               lic.embedding_vec,
               ROW_NUMBER() OVER (PARTITION BY lic.licitacion_id ORDER BY lic.id) AS rn
        FROM public.licitacion_chunk lic
        WHERE lic.embedding_vec IS NOT NULL
          AND lic.licitacion_id = ANY(:ids)
    """), {"ids": cand_ids}).fetchall()

    buckets: Dict[int, List[np.ndarray]] = {}
    for licitacion_id, v, rn in rows:
        if rn > MAX_CAND_PER_LIC_CHUNKS:
            continue
        nv = _to_np_vec(v)
        if nv is None:
            continue
        buckets.setdefault(int(licitacion_id), []).append(_l2_normalize(nv))

    out: Dict[int, np.ndarray] = {}
    for lid, vecs in buckets.items():
        if vecs:
            mean = np.vstack(vecs).mean(axis=0)
            out[lid] = _l2_normalize(mean)
    return out


# ============================================================
# Cosine + penalizaciones
# ============================================================

def _cosine_distance(a: np.ndarray, b: np.ndarray) -> float:
    return float(1.0 - float(np.dot(_l2_normalize(a), _l2_normalize(b))))


def _penalty(meta_t: Dict, meta_c: Tuple[int, Optional[str], Optional[str], Optional[str], Optional[float]]) -> float:
    # meta_c = (id, modalidad, act_econ, estado, cuantia)
    _, _, _, est, _ = meta_c
    p = 0.0
    # Si usas filtro fuerte por modalidad/act_econ, ya vienen iguales; aquí solo penalizamos estado.
    if meta_t.get("estado") and est and str(meta_t["estado"]).strip() != str(est).strip():
        p += PENALTY_ESTADO
    return p


# ============================================================
# Resultado
# ============================================================

@dataclass
class FlagPrecioResult:
    licitacion_id: int
    n_comparables: int
    method: str
    stats: RobustStats
    target_cuantia: Optional[float]
    neighbor_ids: List[int]


# ============================================================
# API principal
# ============================================================

def run_flag_precio_for_one(
    session: Session,
    licitacion_id: int,
    top_k: int = TOP_K,
    min_neighbors: int = MIN_NEIGHBORS_FOR_STATS,
    penalty_estado: float = PENALTY_ESTADO,
) -> FlagPrecioResult:
    """Calcula outlier de precio por comparables usando docvec (chunks) + coseno."""
    global PENALTY_ESTADO
    PENALTY_ESTADO = penalty_estado  # permite tunear por parámetro

    target: Licitacion | None = session.get(Licitacion, licitacion_id)
    if not target:
        raise ValueError(f"Licitación {licitacion_id} no existe")

    # Asegura la definición del flag
    _ensure_flag(session)

    # 1) DocVec target
    t_vec = _fetch_target_docvec(session, licitacion_id)
    t_meta = _fetch_target_meta(session, licitacion_id)
    t_cuantia = t_meta.get("cuantia")
    if t_vec is None:
        repo.set_flag_for_licitacion(
            session=session,
            licitacion_id=licitacion_id,
            flag_codigo="red_precio",
            valor=False,
            comentario="Sin embedding/chunks válidos para el target; no se puede calcular similitud con comparables.",
            fuente="flag_precio(skip)",
            usuario_log="pipeline",
        )
        empty = RobustStats(0, 0, 0, 0, 0, 0, 0, 0, 0)
        return FlagPrecioResult(licitacion_id, 0, "skip", empty, t_cuantia, [])

    # 2) Candidatas + doc_vec
    cands = _fetch_candidate_headers(session, licitacion_id, t_meta)  # [(id,mod,act,est,cuantia)]
    cand_ids = [int(r[0]) for r in cands]
    cand_docvecs = _fetch_candidate_chunks_docvecs(session, cand_ids)

    # 3) Coseno + penalización
    scored: List[Tuple[int, float, Optional[float]]] = []  # (id, score, cuantia)
    for tup in cands:
        cid = int(tup[0])
        c_vec = cand_docvecs.get(cid)
        if c_vec is None:
            continue
        d = _cosine_distance(t_vec, c_vec)
        s = d + _penalty(t_meta, tup)  # coseno + penalización (estado)
        cuant = float(tup[4]) if tup[4] is not None else float("nan")
        scored.append((cid, s, cuant))

    if not scored:
        repo.set_flag_for_licitacion(
            session=session,
            licitacion_id=licitacion_id,
            flag_codigo="red_precio",
            valor=False,
            comentario="No hay comparables con doc_vec (chunks) válidos para estimar precio.",
            fuente="flag_precio_chunks_mean_cos(skip)",
            usuario_log="pipeline",
        )
        empty = RobustStats(0, 0, 0, 0, 0, 0, 0, 0, 0)
        return FlagPrecioResult(licitacion_id, 0, "skip", empty, t_cuantia, [])

    scored.sort(key=lambda x: x[1])  # menor score = más similar
    top = scored[:max(top_k, min_neighbors)]

    # 4) Estadística robusta en cuantía de vecinos
    vec_cuantias = np.array([x[2] for x in top], dtype=float)
    mask = ~np.isnan(vec_cuantias)
    vec_cuantias = vec_cuantias[mask]
    vec_ids = [int(top[i][0]) for i in range(len(top)) if mask[i]]

    if vec_cuantias.size < min_neighbors:
        repo.set_flag_for_licitacion(
            session=session,
            licitacion_id=licitacion_id,
            flag_codigo="red_precio",
            valor=False,
            comentario=f"Solo {vec_cuantias.size} comparables con cuantía; se requieren ≥ {min_neighbors} para evaluación robusta.",
            fuente="flag_precio_chunks_mean_cos(skip)",
            usuario_log="pipeline",
        )
        empty = RobustStats(0, 0, 0, 0, 0, 0, 0, 0, 0)
        return FlagPrecioResult(licitacion_id, int(vec_cuantias.size), "chunks_mean_cos", empty, t_cuantia, vec_ids)

    stats = _robust_stats(vec_cuantias, t_cuantia if t_cuantia is not None else math.nan)

    # 5) Comentario + persistencia del flag
    if t_cuantia is None or not np.isfinite(t_cuantia):
        valor_flag = False
        comentario = "Licitación sin cuantía; no se evalúa outlier de precio."
    else:
        out_iqr = (t_cuantia < stats.lower) or (t_cuantia > stats.upper)
        out_mad = abs(stats.z_mad) >= Z_MAD_THRESHOLD
        valor_flag = bool(out_iqr or out_mad)

        # % desvío vs mediana (si mediana>0)
        dev_pct = None
        if stats.median and np.isfinite(stats.median) and stats.median != 0:
            dev_pct = 100.0 * (t_cuantia - stats.median) / stats.median
        dev_str = (f"{dev_pct:+.1f}%" if dev_pct is not None and np.isfinite(dev_pct) else "—")

        if valor_flag:
            comentario = (
                "Posible outlier de precio: "
                f"cuantía={_fmt_money(t_cuantia)}; mediana={_fmt_money(stats.median)}; "
                f"desvío={dev_str}; IQR=[{_fmt_money(stats.lower)},{_fmt_money(stats.upper)}]; "
                f"zMAD={stats.z_mad:.2f} (umbral {Z_MAD_THRESHOLD}); "
                f"vecinos={vec_cuantias.size}; método=chunks_mean_cos."
            )
        else:
            comentario = (
                "Precio en rango: "
                f"cuantía={_fmt_money(t_cuantia)} dentro de IQR=[{_fmt_money(stats.lower)},{_fmt_money(stats.upper)}], "
                f"desvío={dev_str}; |zMAD|={abs(stats.z_mad):.2f} (< {Z_MAD_THRESHOLD}); "
                f"vecinos={vec_cuantias.size}; método=chunks_mean_cos."
            )

    repo.set_flag_for_licitacion(
        session=session,
        licitacion_id=licitacion_id,
        flag_codigo="red_precio",
        valor=valor_flag,
        comentario=comentario,
        fuente="flag_precio_chunks_mean_cos",
        usuario_log="pipeline",
    )

    return FlagPrecioResult(
        licitacion_id=licitacion_id,
        n_comparables=int(vec_cuantias.size),
        method="chunks_mean_cos",
        stats=stats,
        target_cuantia=t_cuantia,
        neighbor_ids=vec_ids,
    )


# ============================================================
# Batch runner (opcional)
# ============================================================

def run_flag_precio_batch(
    session: Session,
    where_clause: Optional[str] = None,
    limit: Optional[int] = None,
    top_k: int = TOP_K,
) -> List[Dict]:
    q = select(Licitacion)
    if where_clause:
        q = q.where(text(where_clause))
    if limit:
        q = q.limit(limit)

    out = []
    for lic in session.execute(q).scalars().all():
        try:
            res = run_flag_precio_for_one(session, lic.id, top_k=top_k)
            out.append({
                "licitacion_id": res.licitacion_id,
                "n_comparables": res.n_comparables,
                "method": res.method,
                "median": res.stats.median,
                "z_mad": res.stats.z_mad,
                "lower": res.stats.lower,
                "upper": res.stats.upper,
            })
        except Exception as e:
            out.append({"licitacion_id": lic.id, "error": str(e)})
    return out

# app/pipes/flag_fecha.py
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy import text
from sqlalchemy.orm import Session
from db import repo  # tu repo para registrar flags

FLAG_CODE = "F-GAP-APERT"
FLAG_NAME = "Gap aceptación vs apertura (días hábiles)"
FLAG_DESC = (
    "Diferencia de días hábiles entre la Aceptación de ofertas y la Apertura de Ofertas. "
    "Son {dias} días hábiles que duró el proceso; la regla vigente espera ≤ {threshold} días hábiles."
)

def _ensure_flag(db: Session, threshold: int):
    # guarda la descripción expandida ya con la política vigente
    desc = FLAG_DESC.format(dias="{n}", threshold=threshold)  # placeholder visual
    db.execute(text("""
    DO $$
    DECLARE nid int;
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM public.flags WHERE codigo = :c) THEN
        SELECT COALESCE(MAX(id),0)+1 INTO nid FROM public.flags;
        INSERT INTO public.flags(id,codigo,nombre,descripcion)
        VALUES (nid, :c, :n, :d);
      ELSE
        UPDATE public.flags SET nombre=:n, descripcion=:d WHERE codigo=:c;
      END IF;
    END $$;"""), {"c": FLAG_CODE, "n": FLAG_NAME, "d": desc})

def _business_days(d1: Optional[datetime], d2: Optional[datetime], holidays: set[str] | None) -> Optional[int]:
    if not d1 or not d2:
        return None
    if d2 < d1:
        d1, d2 = d2, d1
    cur = d1.date()
    end = d2.date()
    days = 0
    while cur < end:  # cuenta [d1, d2) en días hábiles
        if cur.weekday() < 5 and (not holidays or cur.isoformat() not in holidays):
            days += 1
        cur += timedelta(days=1)
    return days

def run_flag_fecha_for_one(
    db: Session,
    licitacion_id: int,
    json_override: Optional[Dict[str, Any]] = None
) -> dict:
    """
    json_override opcional:
      {
        "threshold": 5,                    # umbral en días hábiles (default 5)
        "holidays": ["2025-01-01", ...]    # feriados ISO (opcional)
      }
    """
    json_override = json_override or {}
    threshold = int(json_override.get("threshold", 5))      # ← por defecto 5
    holidays = set(json_override.get("holidays", []))

    # 1) Vincula licitacion_id -> lic_ext_id (archivo) -> fechas normalizadas en staging
    row = db.execute(text("""
        SELECT n.archivo,
               n.aceptacion_ofertas_ts,
               n.apertura_ofertas_ts
        FROM public.licitacion_keymap k
        JOIN staging.secop_calendario_norm n
          ON n.archivo::text = k.lic_ext_id
        WHERE k.licitacion_id = :lid
        LIMIT 1;
    """), {"lid": licitacion_id}).fetchone()

    if not row:
        return {"ok": False, "flow": "gap_fechas", "reason": "sin_calendario"}

    dias = _business_days(row.aceptacion_ofertas_ts, row.apertura_ofertas_ts, holidays)
    if dias is None:
        return {"ok": False, "flow": "gap_fechas", "reason": "fechas_incompletas"}

    # 2) Asegura flag y registra
    _ensure_flag(db, threshold)
    comentario = (
        f"Gap: {dias} días hábiles "
        f"(Aceptación: {row.aceptacion_ofertas_ts}, Apertura: {row.apertura_ofertas_ts}; archivo={row.archivo}). "
        f"Son {dias} días hábiles que duró el proceso; se espera ≤ {threshold} días hábiles."
    )

    repo.set_flag_for_licitacion(
        session=db,
        licitacion_id=licitacion_id,
        flag_codigo=FLAG_CODE,
        valor=bool(dias > threshold),  # activa si excede el límite esperado
        comentario=comentario,
        fuente="pipe:gap_fechas",
        usuario_log="pipeline",
    )

    return {
        "ok": True,
        "flow": "gap_fechas",
        "flag_applied": (dias > threshold),
        "detail": {
            "dias_habiles": dias,
            "threshold": threshold,
            "archivo": row.archivo,
            "aceptacion_ofertas_ts": row.aceptacion_ofertas_ts,
            "apertura_ofertas_ts": row.apertura_ofertas_ts,
        },
    }

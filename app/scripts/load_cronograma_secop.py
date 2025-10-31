# /app/scripts/load_cronograma_secop.py
from __future__ import annotations
import argparse, os, re
from typing import Optional, Dict
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from datetime import datetime

DDL = """
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.secop_calendario_raw (
  archivo                  TEXT NOT NULL,
  aceptacion_ofertas_raw   TEXT,
  apertura_ofertas_raw     TEXT,
  fecha_publicacion_raw    TEXT,
  presentacion_ofertas_raw TEXT
);

CREATE TABLE IF NOT EXISTS staging.secop_calendario_norm (
  archivo                  TEXT PRIMARY KEY,
  aceptacion_ofertas_ts    TIMESTAMP NULL,
  apertura_ofertas_ts      TIMESTAMP NULL,
  fecha_publicacion_ts     TIMESTAMP NULL,
  presentacion_ofertas_ts  TIMESTAMP NULL
);
"""

COLMAP: Dict[str, str] = {
    "Archivo": "archivo",
    "Aceptación de ofertas": "aceptacion_ofertas_raw",
    "Apertura de Ofertas": "apertura_ofertas_raw",
    "Fecha de publicación": "fecha_publicacion_raw",
    "Presentación de Ofertas": "presentacion_ofertas_raw",
}

# Meses ES → EN para facilitar parseo
ES2EN = {
    "Ene": "Jan", "Feb": "Feb", "Mar": "Mar", "Abr": "Apr", "May": "May", "Jun": "Jun",
    "Jul": "Jul", "Ago": "Aug", "Sep": "Sep", "Oct": "Oct", "Nov": "Nov", "Dic": "Dec"
}

def normalize_es_datetime(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    # Quitar comillas, bloques y quedarnos con la primera fecha “dd/Mon/yyyy - hh:mm am/pm”
    s = s.replace('"', ' ').replace("'", " ").replace("\n", " ").replace("\r", " ")
    # Cambiar meses ES→EN en abreviatura de 3 letras
    for es, en in ES2EN.items():
        s = re.sub(rf"(?i)\b{es}\b", en, s)

    # Captura fecha con hora
    m = re.search(r"(\d{1,2}/[A-Za-z]{3}/\d{4})\s*-\s*(\d{1,2}:\d{2})\s*(am|pm)", s, flags=re.IGNORECASE)
    if m:
        dt_str = f"{m.group(1)} {m.group(2)} {m.group(3).upper()}"
        try:
            return datetime.strptime(dt_str, "%d/%b/%Y %I:%M %p")
        except Exception:
            pass

    # Solo fecha (sin hora)
    m = re.search(r"(\d{1,2}/[A-Za-z]{3}/\d{4})", s, flags=re.IGNORECASE)
    if m:
        try:
            return datetime.strptime(m.group(1), "%d/%b/%Y")
        except Exception:
            pass

    # Pandas fallback (por si llega “2025-07-21 07:05:00”)
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        return None if pd.isna(dt) else dt.to_pydatetime()
    except Exception:
        return None

def ensure_schema(engine: Engine):
    with engine.begin() as cx:
        cx.execute(text(DDL))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/licita_db"))
    ap.add_argument("--excel", required=True)
    ap.add_argument("--sheet", default=None, help="Nombre o índice de la hoja")
    ap.add_argument("--truncate", action="store_true", help="TRUNCATE staging.secop_calendario_* antes de insertar")
    args = ap.parse_args()

    engine = create_engine(args.dsn)
    ensure_schema(engine)

    # Leer Excel
    df = pd.read_excel(args.excel, sheet_name=args.sheet, engine="openpyxl")
    df = df.rename(columns=COLMAP)

    # Asegurar columnas
    for v in COLMAP.values():
        if v not in df.columns:
            df[v] = None

    raw = df[list(COLMAP.values())].copy()

    with engine.begin() as cx:
        if args.truncate:
            cx.execute(text("TRUNCATE staging.secop_calendario_raw;"))
            cx.execute(text("TRUNCATE staging.secop_calendario_norm;"))

        # Insert raw
        cols = list(raw.columns)
        ph = ", ".join([f":{c}" for c in cols])
        sql = text(f"INSERT INTO staging.secop_calendario_raw ({', '.join(cols)}) VALUES ({ph})")
        cx.execute(sql, raw.where(pd.notnull(raw), None).to_dict(orient="records"))

    # Normalizar y upsert
    norm_rows = []
    for _, r in raw.iterrows():
        archivo = None if pd.isna(r.get("archivo")) else str(r["archivo"]).strip()
        if not archivo:
            continue
        norm_rows.append({
            "archivo": archivo,
            "aceptacion_ofertas_ts": normalize_es_datetime(r.get("aceptacion_ofertas_raw")),
            "apertura_ofertas_ts":   normalize_es_datetime(r.get("apertura_ofertas_raw")),
            "fecha_publicacion_ts":  normalize_es_datetime(r.get("fecha_publicacion_raw")),
            "presentacion_ofertas_ts": normalize_es_datetime(r.get("presentacion_ofertas_raw")),
        })

    if norm_rows:
        with engine.begin() as cx:
            upsert = text("""
                INSERT INTO staging.secop_calendario_norm
                  (archivo, aceptacion_ofertas_ts, apertura_ofertas_ts, fecha_publicacion_ts, presentacion_ofertas_ts)
                VALUES
                  (:archivo, :aceptacion_ofertas_ts, :apertura_ofertas_ts, :fecha_publicacion_ts, :presentacion_ofertas_ts)
                ON CONFLICT (archivo) DO UPDATE SET
                  aceptacion_ofertas_ts = EXCLUDED.aceptacion_ofertas_ts,
                  apertura_ofertas_ts   = EXCLUDED.apertura_ofertas_ts,
                  fecha_publicacion_ts  = EXCLUDED.fecha_publicacion_ts,
                  presentacion_ofertas_ts = EXCLUDED.presentacion_ofertas_ts;
            """)
            cx.execute(upsert, norm_rows)

    print(f">> Filas RAW insertadas: {len(raw)}; normalizadas: {len(norm_rows)}")

if __name__ == "__main__":
    main()

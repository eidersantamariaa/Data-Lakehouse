import pandas as pd
from rapidfuzz import process, fuzz
import unicodedata
import pyarrow as pa
import re
from ingesta import get_spark

# ── 1. FUNCIONES DE NORMALIZACIÓN ──────────────────────────────────────────

def quitar_tildes(texto: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )

def extraer_inicial_apellido(nombre_raw: str) -> tuple[str, str]:
    """
    Siempre devuelve inicial + PRIMER apellido:
      - 'Oihan Sancet'              → ('O', 'Sancet')
      - 'Oihan Sancet Tirapu'       → ('O', 'Sancet')  ← ignora 2º apellido
      - 'Sancet, Oihan'             → ('O', 'Sancet')
      - 'Sancet Tirapu, Oihan'      → ('O', 'Sancet')  ← ignora 2º apellido
    """
    nombre_raw = quitar_tildes(nombre_raw.strip())

    # Formato "Apellido1 [Apellido2], Nombre"
    if ',' in nombre_raw:
        apellidos, nombre = nombre_raw.split(',', 1)
        inicial = nombre.strip()[0].upper()
        primer_apellido = apellidos.strip().split()[0].capitalize()  # ← solo el primero
        return inicial, primer_apellido

    # Formato "Nombre Apellido1 [Apellido2]"
    partes = nombre_raw.split()
    if len(partes) == 1:
        return partes[0][0].upper(), partes[0].capitalize()
    inicial = partes[0][0].upper()

    if len(partes) == 2:
        # Solo un apellido
        primer_apellido = partes[1].capitalize()
    else:
        # 2 o más apellidos → coge el del medio (posición -2 si hay 3 tokens)
        primer_apellido = partes[1].capitalize()  # siempre el segundo token

    return inicial, primer_apellido

def normalize_date(value):
    if value is None:
        return None
    v = str(value).strip()
    # detectar separador
    if "/" in v:
        sep = "/"
    elif "-" in v:
        sep = "-"
    elif "." in v:
        sep = "."
    else:
        return None
    
    parts = v.split(sep)
    if len(parts) != 3:
        return None
    
    if len(parts[0]) == 4:  # formato YYYY/MM/DD
        year, month, day = parts
    elif len(parts[2]) == 4:  # formato DD/MM/YYYY
        day, month, year = parts
    else:
        return None
    
    if not (year.isdigit() and month.isdigit() and day.isdigit()):
        return None
    
    if not (1 <= int(month) <= 12 and 1 <= int(day) <= 31):
        return None

    return f"{day.zfill(2)}{month.zfill(2)}{year}"

def generar_clave(nombre_raw: str, fecha_raw: str) -> str:
    inicial, apellido = extraer_inicial_apellido(nombre_raw)
    fecha = normalize_date(fecha_raw)
    return f"{inicial}{apellido}{fecha}"  # → OSancet25042000


def split_table_ref(table: str) -> tuple[str, str]:
    if "." not in table:
        raise ValueError(f"Formato invalido: usa namespace.tabla, recibido '{table}'")
    return table.rsplit(".", 1)

def load_table_df(catalog, table: str) -> pd.DataFrame:
    ns, tbl = split_table_ref(table)
    return catalog.load_table((ns, tbl)).scan().to_arrow().to_pandas()


def save_table_df(catalog, table_name: str, df: pd.DataFrame):
    target_ns, target_tbl = split_table_ref(table_name)
    df_copy = df.copy()
    for c in df_copy.columns:
        try:
            if df_copy[c].isna().all():
                df_copy[c] = pd.Series([None] * len(df_copy), dtype="string")
        except Exception:
            df_copy[c] = df_copy[c].astype("string")

    arrow_table = pa.Table.from_pandas(df_copy, preserve_index=False)

    try:
        catalog.create_namespace(target_ns)
    except Exception:
        pass

    try:
        catalog.create_table((target_ns, target_tbl), schema=arrow_table.schema)
    except Exception as e:
        print(f"⚠ table create skipped for {table_name}: {e}")

    target_t = catalog.load_table((target_ns, target_tbl))

    try:
        existing_names = [f.name for f in target_t.schema().fields]
        new_names      = list(arrow_table.column_names)
        if set(existing_names) != set(new_names):
            combined = list(dict.fromkeys(existing_names + new_names))
            aligned  = pd.DataFrame()
            for col in combined:
                aligned[col] = df[col] if col in df.columns else pd.NA
            for c in aligned.columns:
                try:
                    if aligned[c].isna().all():
                        aligned[c] = pd.Series([None] * len(aligned), dtype="string")
                except Exception:
                    aligned[c] = aligned[c].astype("string")
            arrow_table = pa.Table.from_pandas(aligned, preserve_index=False)
    except Exception as e:
        print(f"⚠ schema union failed for {table_name}: {e}")

    try:
        upd = target_t.update_schema()
        upd.union_by_name(arrow_table.schema)
        upd.commit()
    except Exception as e:
        print(f"⚠ schema update failed for {table_name}: {e}")

    target_t.overwrite(arrow_table)
    return {"table": table_name, "rows": len(df), "cols": df.columns.tolist()}


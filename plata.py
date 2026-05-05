# limpieza.py
import pandas as pd
import re

from limpieza import normalize_text, normalize_date, normalize_height, normalize_weight, normalize_currency, normalize_position

def from_list(normalizar):
    """Wrapper para columnas que pueden ser lista o string."""
    def _inner(v):
        if isinstance(v, list):
            v = v[0] if len(v) > 0 else None
        return normalizar(v) if v is not None else None
    return _inner

def extraer_nombre_equipo(v):
    if not isinstance(v, list):
        return normalize_text(v) if v else None
    
    for item in v:
        if item is None:
            continue
        # Descartar fechas (YYYY-MM-DD) y números puros
        if re.match(r'^\d{4}-\d{2}-\d{2}$', str(item)):
            continue
        if re.match(r'^\d+$', str(item)):
            continue
        return normalize_text(item)
    
    return None

COLUMNAS_CONFIG = [
    {
        "col_final":  "name",
        "fuentes":    ["tm_name", "ts_strPlayer", "fb_player"],
        "tiebreaker": "tm_name",
        "normalizar": normalize_text,
    },
    {
        "col_final":  "born",
        "fuentes":    ["tm_dateOfBirth", "ts_dateBorn", "fb_born"],
        "tiebreaker": "tm_dateOfBirth",
        "normalizar": normalize_date,
    },
    {
        "col_final":  "nationality",
        "fuentes":    ["tm_citizenship", "ts_strNationality", "fb_nation"],
        "tiebreaker": "tm_citizenship",
        "normalizar": from_list(normalize_text),  # lista
    },
    {
        "col_final":  "position",
        "fuentes":    ["tm_position", "ts_strPosition", "fb_pos"],
        "tiebreaker": "tm_position",
        "normalizar": from_list(normalize_position),  # lista
    },
    {
        "col_final":  "height (cm)",
        "fuentes":    ["tm_height", "ts_strHeight"],
        "tiebreaker": "tm_height",
        "normalizar": normalize_height,
    },
    {
        "col_final":  "team",
        "fuentes":    ["tm_club", "ts_strTeam", "fb_team"],
        "tiebreaker": "tm_club",
        "normalizar": extraer_nombre_equipo,  # lista
    },
    {
        "col_final":  "weight (kg)",
        "fuentes":    ["ts_strWeight"],
        "tiebreaker": "ts_strWeight",
        "normalizar": normalize_weight,  # lista
    },
]

COLUMNAS_NORMALIZAR = [
    {"col": "tm_marketValue", "normalizar": normalize_currency},
    {"col": "fb_age",         "normalizar": lambda v: int(str(v).split("-")[0].strip()) if v and "-" in str(v) else v},
]


def _votar(valores_norm):
    """
    Recibe dict {col: valor_normalizado}.
    Devuelve el valor en el que coinciden >=2 fuentes.
    Si todas difieren devuelve None (el caller pone el tiebreaker).
    """
    conteo = {}
    for v in valores_norm.values():
        if v is None:
            continue
        conteo[v] = conteo.get(v, 0) + 1

    mayoria = [v for v, n in conteo.items() if n >= 2]
    return mayoria[0] if mayoria else None


def limpiar_tabla(df, config=COLUMNAS_CONFIG):
    """
    df: pandas DataFrame con columnas prefijadas (tm_, ts_, fb_...)
    config: lista de dicts con col_final, fuentes, tiebreaker, normalizar

    Para añadir una nueva columna consolidada basta con añadir
    un dict a COLUMNAS_CONFIG.
    """
    df = df.copy()
    cols_a_eliminar = []

    # Paso 1: votar columnas solapadas
    for entrada in config:
        col_final  = entrada["col_final"]
        fuentes    = [f for f in entrada["fuentes"] if f in df.columns]
        tiebreaker = entrada["tiebreaker"]
        normalizar = entrada.get("normalizar", lambda x: x)

        if not fuentes:
            continue

        def resolver_fila(row, fuentes=fuentes, tiebreaker=tiebreaker, normalizar=normalizar):
            # Normalizar cada valor
            valores_norm = {}
            for col in fuentes:
                v = row.get(col)
                if v is not None and v == v:  # filtrar None y NaN
                    try:
                        valores_norm[col] = normalizar(v)
                    except:
                        valores_norm[col] = None

            # Votar
            ganador = _votar(valores_norm)
            if ganador is not None:
                return ganador

            # Tiebreaker
            return valores_norm.get(tiebreaker)

        df[col_final] = df.apply(resolver_fila, axis=1)
        cols_a_eliminar.extend(fuentes)

    df = df.drop(columns=cols_a_eliminar)

    # Paso 2: normalizar columnas únicas
    for entrada in COLUMNAS_NORMALIZAR:
        col = entrada["col"]
        if col not in df.columns:
            continue
        normalizar = entrada["normalizar"]
        df[col] = df[col].apply(lambda v: normalizar(v) if v is not None and v == v else None)

    # Reordenar: ids + cols consolidadas + resto
    cols_ids         = [c for c in df.columns if c.startswith("id_")]
    cols_consolidadas = [e["col_final"] for e in config if e["col_final"] in df.columns]
    cols_resto       = [c for c in df.columns if c not in cols_ids and c not in cols_consolidadas]

    df = df[cols_ids + cols_consolidadas + cols_resto]

    print(f"Columnas consolidadas : {cols_consolidadas}")
    print(f"Total columnas        : {len(df.columns)}")
    print(f"Total filas           : {len(df)}")

    return df

from ingesta import get_spark
spark = get_spark()

tabla_final = spark.table("players.mapping.players_unified").toPandas()
tabla_limpia = limpiar_tabla(tabla_final)

# Antes del createDataFrame
for col in tabla_limpia.columns:
    if tabla_limpia[col].isna().all():
        tabla_limpia[col] = tabla_limpia[col].astype(str)

tabla_limpia = tabla_limpia.where(pd.notnull(tabla_limpia), None)

spark.createDataFrame(tabla_limpia) \
    .writeTo("players.silver.players") \
    .using("iceberg") \
    .createOrReplace()

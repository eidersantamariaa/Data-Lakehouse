# matching.py
import pandas as pd
import json
import math
import re
from rapidfuzz import process, fuzz
from ingesta import get_spark
from funciones_mapeo import extraer_inicial_apellido, generar_clave, quitar_tildes

spark = None

def _ensure_spark():
    global spark
    if spark is None:
        try:
            spark = get_spark()
        except Exception:
            spark = None
    return spark

# Función para fuentes con fecha completa
def clave_fecha_completa(nombre, fecha):
    return generar_clave(nombre, fecha)

# Función para fuentes con solo año
def clave_solo_anio(nombre, fecha):
    anio = str(fecha)[:4] if fecha else ""
    inicial, apellido = extraer_inicial_apellido(nombre)
    return f"{inicial}{apellido}{anio}"  # → OSancet2000


# --- Funciones de clave específicas para equipos --------------------------------
def _normalize_team_name_for_key(nombre_raw: str) -> str:
    if not nombre_raw:
        return ""
    s = quitar_tildes(str(nombre_raw)).lower()
    # eliminar tokens comunes (fc, cf, sc, ac, club, the, de, del, etc.)
    s = re.sub(r"\b(fc|cf|sc|ac|club|the|de|del|str|team)\b", "", s)
    # eliminar todo lo que no sea alfanumérico
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s.upper()


def clave_equipo_fecha(nombre, fecha):
    """Genera clave para equipos usando nombre normalizado + año (si existe).

    - `fecha` puede ser año o fecha; se extrae el primer grupo de 4 dígitos si existe.
    """
    name_key = _normalize_team_name_for_key(nombre)
    year = ""
    if fecha:
        m = re.search(r"(\d{4})", str(fecha))
        if m:
            year = m.group(1)
    return f"{name_key}{year}"


def clave_equipo_solo_anio(nombre, fecha):
    anio = str(fecha)[:4] if fecha else ""
    name_key = _normalize_team_name_for_key(nombre)
    return f"{name_key}{anio}"


def _build_source_frames(*fuentes):
    dfs_con_clave = []
    for df, col_nombre, col_fecha, col_id, prefijo, fn_clave, umbral in fuentes:
        df = df.copy()
        df['_clave'] = df.apply(
            lambda r, fn=fn_clave, cn=col_nombre, cf=col_fecha: fn(r.get(cn, None), r.get(cf, None)), axis=1
        )
        id_col = col_id if col_id else '_clave'
        dfs_con_clave.append((df, id_col, prefijo, umbral))
    return dfs_con_clave


def generar_mapeo_df(*fuentes, umbral=85):
    """Versión pandas de generar_mapeo."""
    dfs_con_clave = _build_source_frames(*fuentes)

    df_base, id_base, prefijo_base, _ = dfs_con_clave[0]
    base_cols = ['_clave', id_base]
    mapeo = df_base[base_cols].drop_duplicates(subset='_clave').copy()
    mapeo = mapeo.rename(columns={id_base: f"id_{prefijo_base}"})
    mapeo = mapeo.rename(columns={'_clave': 'clave_canonica'})
    # `id_propio` should be the canonical clave (inicial+apellido+fecha)
    mapeo['id_propio'] = mapeo['clave_canonica']
    # keep the source id column for the base prefix
    mapeo[f'id_{prefijo_base}'] = mapeo[f'id_{prefijo_base}']

    for df_other, id_other, prefijo_other, umbral_fuente in dfs_con_clave[1:]:
        umbral_local = umbral_fuente if umbral_fuente is not None else umbral
        df_unique = df_other.drop_duplicates(subset='_clave').copy()
        if id_other == '_clave':
            df_unique[f'id_{prefijo_other}'] = df_unique['_clave']
        else:
            df_unique[f'id_{prefijo_other}'] = df_unique[id_other]

        if f'id_{prefijo_other}' not in mapeo.columns:
            mapeo[f'id_{prefijo_other}'] = None

        for _, row in df_unique.iterrows():
            clave = row.get('_clave')
            if not clave:
                continue

            matched_key = None
            if not mapeo.empty:
                resultado = process.extractOne(clave, mapeo['clave_canonica'].tolist(), scorer=fuzz.ratio)
                if resultado and resultado[1] >= umbral_local:
                    matched_key = resultado[0]

            if matched_key is not None:
                idx = mapeo.index[mapeo['clave_canonica'] == matched_key][0]
                if pd.isna(mapeo.at[idx, f'id_{prefijo_other}']):
                    mapeo.at[idx, f'id_{prefijo_other}'] = row[f'id_{prefijo_other}']
            else:
                new_row = {col: None for col in mapeo.columns}
                new_row['clave_canonica'] = clave
                # use the canonical clave as id_propio for the added row
                new_row['id_propio'] = clave
                new_row[f'id_{prefijo_other}'] = row[f'id_{prefijo_other}']
                mapeo = pd.concat([mapeo, pd.DataFrame([new_row])], ignore_index=True)

    mapeo = mapeo.drop(columns=['clave_canonica'])
    cols = ['id_propio'] + [c for c in mapeo.columns if c != 'id_propio']
    return mapeo[cols]


def unir_fuentes_df(mapeo, *fuentes):
    resultado = mapeo.copy()

    for df, col_id_df, prefijo in fuentes:
        col_id_mapeo = f"id_{prefijo}"
        if col_id_mapeo not in mapeo.columns:
            raise ValueError(f"El mapeo no contiene la columna '{col_id_mapeo}'")
        df_prefixed = (
            df.copy()
              .add_prefix(f"{prefijo}_")
              .rename(columns={f"{prefijo}_{col_id_df}": col_id_mapeo})
        )
        resultado = resultado.merge(df_prefixed, on=col_id_mapeo, how='left')

    # Resumen ANTES de eliminar columnas de IDs
    total = len(resultado)
    print(f"\nTotal filas en tabla unificada: {total}")
    for _, col_id_df, prefijo in fuentes:
        col_id_mapeo = f"id_{prefijo}"
        if col_id_mapeo in resultado.columns:
            n = resultado[col_id_mapeo].notna().sum()
            print(f"  Con datos de {prefijo}: {n} ({100*n/total:.1f}%)")

    # Ahora sí eliminar
    cols_ids_apis = [c for c in resultado.columns if c.startswith('id_') and c != 'id_propio']
    resultado = resultado.drop(columns=cols_ids_apis)
    resultado = resultado.rename(columns={'id_propio': 'id'})
    return resultado[['id'] + [c for c in resultado.columns if c != 'id']]

def generar_mapeo(*fuentes, umbral=85):
    """
    Genera la tabla de mapeo de IDs entre N fuentes.

    Cada fuente es una tupla:
        (df, col_nombre, col_fecha, col_id, prefijo, fn_clave, umbral)

    col_id puede ser None si la fuente no tiene ID propio (ej. fbref).
    En ese caso se usa la clave generada como identificador.

    Ejemplo:
        generar_mapeo(
            (df_tm,    "name",      "dateOfBirth", "id",       "tm", clave_fecha_completa, 90),
            (df_ts,    "strPlayer", "dateBorn",    "idPlayer", "ts", clave_fecha_completa, 90),
            (df_fbref, "player",    "born",        None,       "fb", clave_solo_anio,      75),
        )
    """
    mapeo = generar_mapeo_df(*fuentes, umbral=umbral)

    # Resumen
    total = len(mapeo)
    print(f"Total jugadores únicos: {total}")
    for _, _, prefijo, _ in _build_source_frames(*fuentes):
        n = mapeo[f'id_{prefijo}'].notna().sum()
        print(f"  Con id_{prefijo}: {n} ({100*n/total:.1f}%)")

    spark_runtime = _ensure_spark()
    if spark_runtime is None:
        return mapeo

    filas_json = [
        json.dumps({col: (None if (isinstance(row[col], float) and math.isnan(row[col])) else row[col]) for col in mapeo.columns})
        for _, row in mapeo.iterrows()
    ]
    return spark_runtime.read.json(spark_runtime.sparkContext.parallelize(filas_json))


def unir_fuentes(mapeo, *fuentes):
    """
    Une los datos de todas las fuentes usando el mapeo de IDs.

    Cada fuente es una tupla:
        (df, col_id_en_df, prefijo)

    col_id_en_df debe coincidir con la columna usada en generar_mapeo.
    Si la fuente no tenía ID (ej. fbref), pasar '_clave' o la clave generada.

    Ejemplo:
        unir_fuentes(
            mapeo,
            (df_tm,    "id",       "tm"),
            (df_ts,    "idPlayer", "ts"),
            (df_fbref, "clave",    "fb"),
        )
    """
    if hasattr(mapeo, "toPandas"):
        mapeo = mapeo.toPandas()

    resultado = unir_fuentes_df(mapeo, *fuentes)  # ya imprime el resumen

    spark_runtime = _ensure_spark()
    if spark_runtime is None:
        return resultado

    def limpiar(v):
        if isinstance(v, float) and math.isnan(v): return None
        if isinstance(v, dict): return {k: limpiar(val) for k, val in v.items()}
        if isinstance(v, list): return [limpiar(i) for i in v]
        return v

    filas_json = [
        json.dumps({col: limpiar(row[col]) for col in resultado.columns})
        for _, row in resultado.iterrows()
    ]
    return spark_runtime.read.json(spark_runtime.sparkContext.parallelize(filas_json))

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
    s = re.sub(r"\b(fc|cf|sc|ac|club|the|de|del|str|team)\b", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s.upper()


def clave_equipo_fecha(nombre, fecha):
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


# ── Aplanado de valores complejos ────────────────────────────────────────────
# Convierte dicts/listas en strings comparables para que detectar_solapamientos
# pueda hacer fuzzy matching sobre ellos.  Se aplica en unir_fuentes_df antes
# de devolver el resultado, de modo que limpieza.py recibe siempre strings planos.

def _aplanar_valor(v):
    """
    Convierte un valor complejo (dict, list, ndarray) en un string legible.
    Devuelve scalars sin modificar.

    Ejemplos:
        {'name': 'Napoli', 'id': '6195'}  →  'name=Napoli, id=6195'
        ['Spain', 'Argentina']            →  'Spain, Argentina'
        'Naples'                          →  'Naples'   (sin cambios)
    """
    if isinstance(v, dict):
        return ", ".join(
            f"{k}={_aplanar_valor(val)}"
            for k, val in v.items()
            if val is not None
        )
    if isinstance(v, (list, tuple, set)):
        return ", ".join(str(i) for i in v if i is not None)
    # numpy arrays y similares
    if hasattr(v, "tolist") and not isinstance(v, (str, bytes)):
        try:
            return _aplanar_valor(v.tolist())
        except Exception:
            return str(v)
    return v


def _es_nulo(v):
    """Evalúa nulos sin romper cuando el valor es array/lista."""
    if v is None:
        return True
    if isinstance(v, (list, tuple, dict, set)):
        return False
    try:
        na = pd.isna(v)
        return bool(na) if isinstance(na, bool) else False
    except Exception:
        return False


def _aplanar_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplana in-place todas las columnas de `df` que contengan dicts, listas
    o arrays en alguna de sus filas.  El resto de columnas no se tocan.
    """
    # Paso 1: expandir todos los dicts en columnas nuevas
    for col in list(df.columns):
        serie = df[col].dropna()
        if not serie.empty and serie.apply(lambda x: isinstance(x, dict)).any():
            df = _expandir_dict_col(df, col)

    # Paso 2: aplanar listas en TODAS las columnas,
    # incluyendo las recién creadas en el paso 1
    for col in list(df.columns):
        serie = df[col].dropna()
        if not serie.empty and serie.apply(
            lambda x: isinstance(x, (list, tuple, set))
            or (hasattr(x, "tolist") and not isinstance(x, (str, bytes)))  # ← numpy arrays
        ).any():
            df[col] = df[col].apply(
                lambda v: extraer_representativo(v) if not _es_nulo(v) else None
            )

    return df


# ── Pipeline de matching ─────────────────────────────────────────────────────

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
    mapeo['id_propio'] = mapeo['clave_canonica']
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

    # Eliminar columnas de IDs auxiliares
    cols_ids_apis = [c for c in resultado.columns if c.startswith('id_') and c != 'id_propio']
    resultado = resultado.drop(columns=cols_ids_apis)
    resultado = resultado.rename(columns={'id_propio': 'id'})
    resultado = resultado[['id'] + [c for c in resultado.columns if c != 'id']]

    # ── APLANADO ──────────────────────────────────────────────────────────────
    # Convertir dicts/listas en strings antes de devolver el DataFrame, para que
    # detectar_solapamientos y el fuzzy matching de limpieza.py funcionen
    # correctamente. Ejemplo:
    #   {'contractExpires':'2030-06-30','name':'Napoli'}  →  'contractExpires=2030-06-30, name=Napoli'
    resultado = _aplanar_df(resultado)

    return resultado


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

    resultado = unir_fuentes_df(mapeo, *fuentes)  # ya imprime el resumen + aplana

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

import re

# Claves que suelen contener el nombre principal en cualquier API deportiva
_CLAVES_NOMBRE = {
    "name", "nombre", "player", "team", "club", "title",
    "label", "strname", "strplayer", "strteam", "fullname"
}

def _parece_nombre(v):
    """True si el string parece un nombre y no una fecha, ID o número."""
    s = str(v).strip()
    if not s or s.lower() == "none":
        return False
    if re.fullmatch(r"\d+", s):               # ID numérico
        return False
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", s):  # fecha
        return False
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s[:10]):
        return False
    return True

def extraer_representativo(v):
    """
    Extrae el string más representativo de un valor complejo.
    
    dict  → busca clave nombre conocida, si no la hay toma
            el valor string más largo que parezca un nombre
    list  → primer elemento que parezca un nombre
    resto → devuelve tal cual
    """
    if isinstance(v, dict):
        # 1. Buscar clave prioritaria (case-insensitive)
        for k, val in v.items():
            if k.lower() in _CLAVES_NOMBRE and val and _parece_nombre(val):
                return str(val).strip()
        # 2. Fallback: el string más largo que parezca un nombre
        candidatos = [
            str(val).strip()
            for val in v.values()
            if val is not None and _parece_nombre(val)
        ]
        return max(candidatos, key=len) if candidatos else None

    if isinstance(v, (list, tuple, set)):
        for item in v:
            if item is not None and _parece_nombre(str(item)):
                return str(item).strip()
        return None

    if hasattr(v, "tolist") and not isinstance(v, (str, bytes)):
        return extraer_representativo(v.tolist())

    return v

def _expandir_dict_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Si col contiene dicts, crea nuevas columnas col_key1, col_key2...
    y elimina la original.
    """
    muestra = df[col].dropna()
    if not muestra.apply(lambda x: isinstance(x, dict)).any():
        return df

    expandida = df[col].apply(
        lambda v: v if isinstance(v, dict) else {}
    ).apply(pd.Series).add_prefix(f"{col}_")

    # Solo columnas con al menos un valor no nulo
    expandida = expandida.loc[:, expandida.notna().any()]

    return pd.concat([df.drop(columns=[col]), expandida], axis=1)
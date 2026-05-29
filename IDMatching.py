# matching.py
import pandas as pd
import json
import math
import re
from rapidfuzz import process, fuzz
from ingesta import get_spark
from funciones_mapeo import extraer_inicial_apellido, generar_clave, quitar_tildes
from limpieza import normalize_text, normalize_date, normalize_height, normalize_weight, normalize_currency, normalize_position

spark = None

def _ensure_spark():
    global spark
    if spark is None:
        try:
            spark = get_spark()
        except Exception:
            spark = None
    return spark


# ── Helpers de carga / guardado ──────────────────────────────────────────────

def _load_table(table_name: str, spark=None):
    """
    Carga una tabla como DataFrame pandas.
    - Si se pasa spark, usa spark.table().toPandas().
    - Si no, intenta usar el catalog de ui.py como fallback.
    """
    if spark is not None:
        return spark.table(table_name).toPandas()
    try:
        from ui import _load_table_df
        return _load_table_df(table_name)
    except Exception:
        raise RuntimeError(
            f"No se pudo cargar '{table_name}': pasa una SparkSession o conecta el catalog."
        )

def _pandas_to_spark_safe(spark, df: pd.DataFrame):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

    df_copy = df.copy()
    for c in df_copy.columns:
        try:
            serie = df_copy[c].dropna()
            if df_copy[c].isna().all():
                df_copy[c] = pd.Series([None] * len(df_copy), dtype="string")
            elif not serie.empty and serie.apply(
                lambda x: isinstance(x, (dict, list, tuple, set))
            ).any():
                df_copy[c] = df_copy[c].apply(
                    lambda v: json.dumps(v) if isinstance(v, (dict, list, tuple, set))
                    else (None if _es_nulo(v) else v)
                )
        except Exception:
            df_copy[c] = df_copy[c].astype("string")

    fields = []
    for c in df_copy.columns:
        dtype = df_copy[c].dtype
        if dtype in ("float64", "float32"):
            fields.append(StructField(c, DoubleType(), True))
        elif dtype in ("int64", "int32"):
            fields.append(StructField(c, LongType(), True))
        else:
            df_copy[c] = df_copy[c].astype("object").where(df_copy[c].notna(), None)
            fields.append(StructField(c, StringType(), True))

    return spark.createDataFrame(df_copy, StructType(fields))


def _save_table(table_name: str, df: pd.DataFrame, spark=None):
    if spark is not None:
        (
            _pandas_to_spark_safe(spark, df)
                .writeTo(table_name)
                .using("iceberg")
                .createOrReplace()
        )
        return table_name
    try:
        from ui import _save_table_df
        return _save_table_df(table_name, df)
    except Exception:
        raise RuntimeError(
            f"No se pudo guardar '{table_name}': pasa una SparkSession o conecta el catalog."
        )

# ── Función principal ────────────────────────────────────────────────────────

def run_matching(sources: list, spark=None, umbral: float = 85,
                 mapping_table: str = None, unified_table: str = None):
    """
    Orquesta el flujo completo de matching entre N fuentes.

    Parámetros
    ----------
    sources : list[dict]
        Cada dict describe una fuente con las claves:
            table     : nombre completo de la tabla (ej. "players.mock.transfermarkt_players")
            name_col  : columna con el nombre del jugador/equipo/etc.
            date_col  : columna con la fecha de nacimiento/fundación/etc.
            id_col    : columna de ID propio (None si la fuente no tiene ID nativo)
            prefix    : prefijo corto para identificar la fuente (ej. "tm", "ts", "fb")
            key_mode  : "full_date" (por defecto) o "year"
            umbral    : umbral de similitud específico para esta fuente (hereda el global si no se indica)
    spark : SparkSession, opcional
        Si se pasa, se usa para cargar y guardar tablas. Si no, se usa el catalog.
    umbral : float
        Umbral global de similitud para el fuzzy matching (por defecto 85).
    mapping_table : str, opcional
        Nombre de tabla donde guardar el mapeo de IDs.
    unified_table : str, opcional
        Nombre de tabla donde guardar la tabla unificada.

    Retorna
    -------
    (mapeo, tabla_final) : tuple[pd.DataFrame, pd.DataFrame]

    Ejemplo
    -------
    sources = [
        {"table": "players.mock.transfermarkt_players", "name_col": "name",
         "date_col": "dateOfBirth", "id_col": "id", "prefix": "tm",
         "key_mode": "full_date", "umbral": 88},
        {"table": "players.mock.fbref_players", "name_col": "player",
         "date_col": "born", "id_col": None, "prefix": "fb",
         "key_mode": "year", "umbral": 75},
    ]
    mapeo, tabla_final = run_matching(sources, spark=spark, umbral=85,
                                      mapping_table="players.mock.players_mapping",
                                      unified_table="players.mock.players_unified")
    """

    # ── Paso 1: cargar tablas y construir source_frames ───────────────────────
    source_frames = []
    for src in sources:
        name_col   = src["name_col"]
        date_col   = src["date_col"]
        id_col     = src.get("id_col") or None
        prefix     = src["prefix"]
        key_mode   = src.get("key_mode", "full_date")
        src_umbral = float(src.get("umbral", umbral))
        key_fn     = clave_solo_anio if key_mode == "year" else clave_fecha_completa

        df = _load_table(src["table"], spark)
        source_frames.append((df, name_col, date_col, id_col, prefix, key_fn, src_umbral))

    # ── Paso 2: generar mapeo de IDs ──────────────────────────────────────────
    mapeo = generar_mapeo_df(*source_frames, umbral=umbral)
    if mapping_table:
        _save_table(mapping_table, mapeo, spark)

    # ── Paso 3: preparar source_join_frames ───────────────────────────────────
    # Para fuentes sin id_col nativo se genera _clave como ID sintético,
    # igual que hace el endpoint matching_run de ui.py.
    source_join_frames = []
    for src, frame in zip(sources, source_frames):
        df       = frame[0].copy()
        name_col = src["name_col"]
        date_col = src["date_col"]
        id_col   = src.get("id_col") or None
        prefix   = src["prefix"]
        key_mode = src.get("key_mode", "full_date")
        key_fn   = clave_solo_anio if key_mode == "year" else clave_fecha_completa

        if not id_col:
            df["_clave"] = df.apply(
                lambda r, fn=key_fn, cn=name_col, cf=date_col: fn(r.get(cn), r.get(cf)),
                axis=1,
            )
            id_col = "_clave"

        source_join_frames.append((df, id_col, prefix))

    # ── Paso 4: unir fuentes ──────────────────────────────────────────────────
    tabla_final = unir_fuentes_df(mapeo, *source_join_frames)
    if unified_table:
        _save_table(unified_table, tabla_final, spark)

    return mapeo, tabla_final


# --- Funciones de clave específicas para jugadores --------------------------------

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


# --- Funciones de clave específicas para ligas --------------------------------
def _normalize_league_name_for_key(nombre_raw: str) -> str:
    """Normaliza nombres de ligas para generar claves comparables.

    Elimina palabras comunes como 'league', 'liga', 'serie', 'premier',
    quita tildes y signos, y devuelve una cadena uppercase sin espacios.
    """
    if not nombre_raw:
        return ""
    s = quitar_tildes(str(nombre_raw)).lower()
    s = re.sub(r"\b(spanish|english|german|italian|french|scottish|dutch|irish)\b", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s.upper()


def clave_liga(nombre: str, pais: str) -> str:
    """Genera una clave canónica para una liga a partir de su nombre y país.

    Ejemplo: ('Spanish La Liga', 'Spain') -> 'LALIGAESPAN'
    """
    name_key = _normalize_league_name_for_key(nombre)
    country_key = quitar_tildes(str(pais)).upper() if pais else ""
    country_key = re.sub(r"[^A-Z0-9]+", "", country_key)
    return f"{name_key}{country_key}"


# ── Aplanado de valores complejos ────────────────────────────────────────────

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
    o arrays en alguna de sus filas. El resto de columnas no se tocan.
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
            or (hasattr(x, "tolist") and not isinstance(x, (str, bytes)))
        ).any():
            df[col] = df[col].apply(
                lambda v: extraer_representativo(v) if not _es_nulo(v) else None
            )

    return df


# ── Métodos de normalización ──────────────────────────────────────────────────

def _inferir_normalizador(col: str):
    """Infiere la función de normalización por el nombre de la columna."""
    c = col.lower()
    if any(x in c for x in ("height", "altura")):
        return normalize_height
    if any(x in c for x in ("weight", "peso")):
        return normalize_weight
    if any(x in c for x in ("value", "price", "salary", "wage", "marketvalue", "marketValue", "market_value")):
        return normalize_currency
    if any(x in c for x in ("date", "born", "birth", "dob")):
        return normalize_date
    if any(x in c for x in ("position", "pos")):
        return normalize_position
    return normalize_text


def _normalizar_df(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if col == 'id':
            continue
        if df[col].dtype not in (object, "string"):
            continue
        serie = df[col].dropna()
        if serie.empty:
            continue
        fn = _inferir_normalizador(col)
        df[col] = df[col].apply(
            lambda v: fn(v) if not _es_nulo(v) else None
        )
    return df


# ── Pipeline de matching ──────────────────────────────────────────────────────

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

    resultado = _aplanar_df(resultado)
    resultado = _normalizar_df(resultado)

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
    print(f"Total registros únicos: {total}")
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

    resultado = unir_fuentes_df(mapeo, *fuentes)

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


# ── Helpers de representación y expansión ────────────────────────────────────

_CLAVES_NOMBRE = {
    "name", "nombre", "player", "team", "club", "title",
    "label", "strname", "strplayer", "strteam", "fullname"
}

def _parece_nombre(v):
    """True si el string parece un nombre y no una fecha, ID o número."""
    s = str(v).strip()
    if not s or s.lower() == "none":
        return False
    if re.fullmatch(r"\d+", s):
        return False
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", s):
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
        for k, val in v.items():
            if k.lower() in _CLAVES_NOMBRE and val and _parece_nombre(val):
                return str(val).strip()
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

    expandida = expandida.loc[:, expandida.notna().any()]

    return pd.concat([df.drop(columns=[col]), expandida], axis=1)
# matching.py
import pandas as pd
import json
import math
from rapidfuzz import process, fuzz
from ingesta import get_spark

spark = get_spark()


def _generar_clave(nombre, fecha_o_anio):
    """Genera clave normalizada: nombre_sin_espacios + _ + año"""
    if not nombre:
        return None
    nombre_norm = str(nombre).lower().strip().replace(" ", "")
    anio = str(fecha_o_anio)[:4] if fecha_o_anio else ""
    return f"{nombre_norm}_{anio}"


def generar_mapeo(*fuentes, umbral=85):
    """
    Genera la tabla de mapeo de IDs entre N fuentes.

    Cada fuente es una tupla:
        (df, col_nombre, col_fecha, col_id, prefijo)

    col_id puede ser None si la fuente no tiene ID propio (ej. fbref).
    En ese caso se usa la clave generada como identificador.

    Ejemplo:
        generar_mapeo(
            (df_tm,    "name",      "dateOfBirth", "id",       "tm"),
            (df_ts,    "strPlayer", "dateBorn",    "idPlayer", "ts"),
            (df_fbref, "player",    "born",        None,       "fb"),
        )
    """
    # 1. Generar clave en cada fuente
    dfs_con_clave = []
    for df, col_nombre, col_fecha, col_id, prefijo in fuentes:
        df = df.copy()
        df['_clave'] = df.apply(
            lambda r: _generar_clave(r[col_nombre], r[col_fecha]), axis=1
        )
        # Si no tiene ID propio, usamos la clave como ID
        id_col = col_id if col_id else '_clave'
        dfs_con_clave.append((df, id_col, prefijo))

    # 2. Tomar la primera fuente como base
    df_base, id_base, prefijo_base = dfs_con_clave[0]
    mapeo = df_base[['_clave', id_base]].rename(
        columns={id_base: f"id_{prefijo_base}"}
    )

    # 3. Hacer fuzzy matching contra cada fuente adicional
    for df_other, id_other, prefijo_other in dfs_con_clave[1:]:
        claves_other = df_other['_clave'].tolist()

        def buscar_match(clave, umbral=umbral):
            if not clave:
                return None
            resultado = process.extractOne(clave, claves_other, scorer=fuzz.ratio)
            if resultado and resultado[1] >= umbral:
                return resultado[0]
            return None

        mapeo[f'_clave_{prefijo_other}'] = mapeo['_clave'].apply(buscar_match)

        if id_other == '_clave':
            df_unique = df_other.drop_duplicates(subset='_clave')
            lookup = pd.Series(df_unique['_clave'].values, index=df_unique['_clave'].values)
        else:
            df_unique = df_other.drop_duplicates(subset='_clave')
            lookup = df_unique.set_index('_clave')[id_other]
        mapeo[f'id_{prefijo_other}'] = mapeo[f'_clave_{prefijo_other}'].map(lookup)
        mapeo = mapeo.drop(columns=[f'_clave_{prefijo_other}'])

    mapeo = mapeo.drop(columns=['_clave'])

    # Resumen
    total = len(mapeo)
    print(f"Total jugadores únicos: {total}")
    for _, _, prefijo in dfs_con_clave:
        n = mapeo[f'id_{prefijo}'].notna().sum()
        print(f"  Con id_{prefijo}: {n} ({100*n/total:.1f}%)")

    return mapeo


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

        resultado = resultado.merge(df_prefixed, on=col_id_mapeo, how="left")

    # Resumen
    total = len(resultado)
    print(f"\nTotal filas en tabla unificada: {total}")
    for _, _, prefijo in fuentes:
        n = resultado[f"id_{prefijo}"].notna().sum()
        print(f"  Con datos de {prefijo}: {n} ({100*n/total:.1f}%)")

    # Serializar a Spark
    def limpiar(v):
        if isinstance(v, float) and math.isnan(v): return None
        if isinstance(v, dict): return {k: limpiar(val) for k, val in v.items()}
        if isinstance(v, list): return [limpiar(i) for i in v]
        return v

    filas_json = [
        json.dumps({col: limpiar(row[col]) for col in resultado.columns})
        for _, row in resultado.iterrows()
    ]

    return spark.read.json(spark.sparkContext.parallelize(filas_json))

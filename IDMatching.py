# IDMatching.py
import pandas as pd
from funciones_mapeo import generar_clave
from rapidfuzz import process, fuzz


def _resolver_columnas_duplicadas(df, columnas_izq, columnas_der, sufijo_izq="_tm", sufijo_der="_ts"):
    """Combina columnas con el mismo nombre priorizando valores de la izquierda."""
    comunes = (set(columnas_izq) & set(columnas_der)) - {"id_transfermarkt", "id_thesportsdb"}

    for col in comunes:
        col_izq = f"{col}{sufijo_izq}"
        col_der = f"{col}{sufijo_der}"
        if col_izq in df.columns and col_der in df.columns:
            df[col] = df[col_izq].combine_first(df[col_der])
            df = df.drop(columns=[col_izq, col_der])

    return df

def run(df1, df2):
    # 1. Generar clave en ambos DataFrames
    df1['id_propio'] = df1.apply(lambda r: generar_clave(r['name'], r['dateOfBirth']), axis=1)
    df2['id_propio'] = df2.apply(lambda r: generar_clave(r['strPlayer'], r['dateBorn']), axis=1)

    # 2. Fuzzy matching para enlazar los que coincidan
    claves_df2 = df2['id_propio'].tolist()

    def buscar_match(clave, umbral=85):
        if not clave:
            return None
        resultado = process.extractOne(clave, claves_df2, scorer=fuzz.ratio)
        if resultado and resultado[1] >= umbral:
            return resultado[0]
        return None

    df1['clave_matched'] = df1['id_propio'].apply(buscar_match)

    # 3. Jugadores de df1 con su match en df2 (si existe)
    df1_mapeo = (
        df1[['id', 'id_propio', 'clave_matched']] 
        .merge(
            df2[['idPlayer', 'id_propio']].rename(columns={'id_propio': 'clave_matched'}),
            on='clave_matched',
            how='left'
        )
        .drop(columns='clave_matched')
        .rename(columns={'id': 'id_transfermarkt', 'idPlayer': 'id_thesportsdb'})
    )

    # 4. Jugadores de df2 que NO están en df1
    ids_ya_matcheados = df1_mapeo['id_thesportsdb'].dropna().tolist()
    df2_solo = (
        df2[~df2['idPlayer'].isin(ids_ya_matcheados)][['idPlayer', 'id_propio']]
        .rename(columns={'idPlayer': 'id_thesportsdb'})
        .assign(id_transfermarkt=None)
    )

    # 5. Unir todo
    mapeo = pd.concat([df1_mapeo, df2_solo], ignore_index=True)

    total = len(mapeo)
    cruzados = mapeo['id_transfermarkt'].notna() & mapeo['id_thesportsdb'].notna()
    print(f"Total jugadores únicos: {total}")
    print(f"Cruzados entre ambas APIs: {cruzados.sum()} ({100*cruzados.sum()/total:.1f}%)")
    print(f"Solo en transfermarkt: {mapeo['id_thesportsdb'].isna().sum()}")
    print(f"Solo en thesportsdb: {mapeo['id_transfermarkt'].isna().sum()}")

    return mapeo


def unir_fuentes_con_mapeo(df_transfermarkt, df_thesportsdb, mapeo_ids):
    """
    Une datos de jugadores partiendo de transfermarkt y enriqueciendo con thesportsdb.

    Reglas:
    1. Base principal: transfermarkt.
    2. Si existe id_thesportsdb para un jugador, se añade la info adicional de thesportsdb.
    3. Si no hay match en thesportsdb, se conserva solo transfermarkt.
    4. Si un jugador existe solo en thesportsdb (segun mapeo), tambien se incluye.
    """
    if mapeo_ids is None or mapeo_ids.empty:
        raise ValueError("mapeo_ids no puede ser None ni estar vacio")

    # Copias defensivas para no modificar DataFrames de entrada.
    tm = df_transfermarkt.copy()
    ts = df_thesportsdb.copy()
    mapeo = mapeo_ids.copy()

    if "id" not in tm.columns:
        raise ValueError("df_transfermarkt debe contener la columna 'id'")
    if "idPlayer" not in ts.columns:
        raise ValueError("df_thesportsdb debe contener la columna 'idPlayer'")
    if not {"id_transfermarkt", "id_thesportsdb"}.issubset(mapeo.columns):
        raise ValueError("mapeo_ids debe contener 'id_transfermarkt' e 'id_thesportsdb'")

    base = mapeo.reset_index(drop=True).copy()
    base["_row_id"] = range(len(base))

    tm_join = base.merge(
        tm,
        left_on="id_transfermarkt",
        right_on="id",
        how="left",
    )
    ts_join = base.merge(
        ts,
        left_on="id_thesportsdb",
        right_on="idPlayer",
        how="left",
    )

    resultado = base.drop(columns=["_row_id"]).copy()

    columnas_tm = [columna for columna in tm.columns if columna != "id"]
    columnas_ts = [columna for columna in ts.columns if columna != "idPlayer"]

    for columna in columnas_tm:
        if columna in resultado.columns:
            continue
        resultado[columna] = tm_join[columna]

    for columna in columnas_ts:
        if columna in resultado.columns:
            resultado[columna] = resultado[columna].combine_first(ts_join[columna])
        else:
            resultado[columna] = ts_join[columna]

    columnas_ordenadas = list(base.drop(columns=["_row_id"]).columns)
    columnas_ordenadas.extend([columna for columna in columnas_tm if columna not in columnas_ordenadas])
    columnas_ordenadas.extend([columna for columna in columnas_ts if columna not in columnas_ordenadas])

    return resultado[columnas_ordenadas]
# IDMatching.py
import pandas as pd
from funciones_mapeo import generar_clave
from rapidfuzz import process, fuzz

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

    tm = df_transfermarkt.copy()
    ts = df_thesportsdb.copy()
    mapeo = mapeo_ids.copy()

    if "id" not in tm.columns:
        raise ValueError("df_transfermarkt debe contener la columna 'id'")
    if "idPlayer" not in ts.columns:
        raise ValueError("df_thesportsdb debe contener la columna 'idPlayer'")
    if not {"id_transfermarkt", "id_thesportsdb"}.issubset(mapeo.columns):
        raise ValueError("mapeo_ids debe contener 'id_transfermarkt' e 'id_thesportsdb'")

    # Prefijar columnas para no perder el origen tras el merge
    tm_prefixed = tm.add_prefix("tm_").rename(columns={"tm_id": "id_transfermarkt"})
    ts_prefixed = ts.add_prefix("ts_").rename(columns={"ts_idPlayer": "id_thesportsdb"})

    # --- Caso 1: solo transfermarkt (id_thesportsdb es NaN) ---
    mapeo_solo_tm = mapeo[mapeo["id_thesportsdb"].isna() & mapeo["id_transfermarkt"].notna()]
    filas_solo_tm = mapeo_solo_tm.merge(tm_prefixed, on="id_transfermarkt", how="left")

    # --- Caso 2: ambos IDs presentes ---
    mapeo_ambos = mapeo[mapeo["id_transfermarkt"].notna() & mapeo["id_thesportsdb"].notna()]
    filas_ambos = (
        mapeo_ambos
        .merge(tm_prefixed, on="id_transfermarkt", how="left")
        .merge(ts_prefixed, on="id_thesportsdb", how="left")
    )

    # --- Caso 3: solo thesportsdb (id_transfermarkt es NaN) ---
    mapeo_solo_ts = mapeo[mapeo["id_transfermarkt"].isna() & mapeo["id_thesportsdb"].notna()]
    filas_solo_ts = mapeo_solo_ts.merge(ts_prefixed, on="id_thesportsdb", how="left")

    # --- Unir los tres grupos ---
    resultado = pd.concat(
        [filas_ambos, filas_solo_tm, filas_solo_ts],
        ignore_index=True,
        sort=False          # mantiene el orden de columnas del primer df
    )

    # Limpiar columnas de mapeo intermedias si no se necesitan
    cols_a_drop = [c for c in ["id_propio"] if c in resultado.columns]
    resultado = resultado.drop(columns=cols_a_drop)

    # Resumen
    total = len(resultado)
    ambos    = resultado["id_transfermarkt"].notna() & resultado["id_thesportsdb"].notna()
    solo_tm  = resultado["id_transfermarkt"].notna() & resultado["id_thesportsdb"].isna()
    solo_ts  = resultado["id_transfermarkt"].isna()  & resultado["id_thesportsdb"].notna()

    print(f"Total jugadores en tabla unificada : {total}")
    print(f"  Cruzados (ambas fuentes)         : {ambos.sum()}   ({100*ambos.sum()/total:.1f}%)")
    print(f"  Solo transfermarkt               : {solo_tm.sum()} ({100*solo_tm.sum()/total:.1f}%)")
    print(f"  Solo thesportsdb                 : {solo_ts.sum()} ({100*solo_ts.sum()/total:.1f}%)")

    return resultado
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

"""
El resultado será algo así:
id_propio        | id_transfermarkt | id_thesportsdb
OSancet25042000  |     1018938      |     34159231    ← en ambas
JMartin23042006  |     1018939      |     NaN         ← solo transfermarkt
DOkereke29081997 |     NaN          |     34161149    ← solo thesportsdb

"""
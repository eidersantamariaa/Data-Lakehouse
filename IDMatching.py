# IDMatching.py
from funciones_mapeo import generar_clave
from rapidfuzz import process, fuzz

def run(df1, df2):
    # 1. Generar clave en df1 (transfermarkt)
    df1['clave'] = df1.apply(lambda r: generar_clave(r['name'], r['dateOfBirth']), axis=1)
    # 2. Generar clave en df2 (thesportsdb)
    df2['clave'] = df2.apply(lambda r: generar_clave(r['strPlayer'], r['dateBorn']), axis=1)

    # 3. Fuzzy matching entre claves
    claves_df2 = df2['clave'].tolist()

    def buscar_match(clave, umbral=85):
        if not clave:
            return None
        resultado = process.extractOne(clave, claves_df2, scorer=fuzz.ratio)
        if resultado and resultado[1] >= umbral:
            return resultado[0]
        return None

    df1['clave_matched'] = df1['clave'].apply(buscar_match)

    # 4. Construir tabla de mapeo
    mapeo = (
        df1[['id', 'clave', 'clave_matched']]
        .merge(
            df2[['idPlayer', 'clave']].rename(columns={'clave': 'clave_matched'}),
            on='clave_matched',
            how='left'
        )
        .rename(columns={
            'clave':    'id_propio',
            'id':       'id_transfermarkt',
            'idPlayer': 'id_thesportsdb'
        })
        .drop(columns='clave_matched')
    )

    sin_match = mapeo['id_thesportsdb'].isna().sum()
    print(f"Matches: {len(mapeo) - sin_match}/{len(mapeo)} ({100*(1-sin_match/len(mapeo)):.1f}%)")

    return mapeo
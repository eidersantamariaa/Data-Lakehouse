from funciones_mapeo import normalizar_nacion
from ingesta import get_spark
from rapidfuzz import process, fuzz
import pandas as pd

spark = get_spark()

def run():
    fusion = spark.read.table("players.mapping.players_united")
    api3   = spark.read.table("players.fbref.players")

    # Convertir a pandas para el fuzzy matching
    df_fusion = fusion.toPandas()
    df_api3   = api3.toPandas()

    # Normalizar naciones
    df_fusion['nation_code'] = df_fusion['ts_strNationality'].apply(normalizar_nacion)
    
    # Extraer año de nacimiento
    df_fusion['born_year'] = df_fusion['tm_dateOfBirth'].str[:4]

    # Generar claves
    df_fusion['clave'] = (
        df_fusion['tm_name'].str.lower().str.replace(" ", "") + "_" + df_fusion['born_year'].fillna("")
    )
    df_api3['clave'] = (
        df_api3['player'].str.lower().str.replace(" ", "") + "_" + df_api3['born'].astype('Int64').astype(str)
    )

    # Fuzzy matching
    claves_api3 = df_api3['clave'].tolist()

    def buscar_match(row, umbral=90):
        resultado = process.extractOne(row['clave'], claves_api3, scorer=fuzz.ratio)
        if resultado and resultado[1] >= umbral:
            clave_matched = resultado[0]
            candidato = df_api3[df_api3['clave'] == clave_matched].iloc[0]
            # Validar nación como filtro secundario
            if pd.notna(row['nation_code']) and pd.notna(candidato['nation']):
                if row['nation_code'] != candidato['nation']:
                    return None
            return clave_matched
        return None

    df_fusion['clave_matched'] = df_fusion.apply(buscar_match, axis=1)

    # Join con las estadísticas de fbref
    resultado = df_fusion.merge(
        df_api3.rename(columns={'clave': 'clave_matched'}),
        on='clave_matched',
        how='left'
    )

    # Guardar
    # Convertir columnas all-null a string para que Spark pueda inferir el tipo
    for col in resultado.columns:
        if resultado[col].isna().all():
            resultado[col] = resultado[col].astype(str)

    # Limpiar tipos problemáticos
    resultado = resultado.where(pd.notnull(resultado), None)
    spark_df = spark.createDataFrame(resultado)
    spark_df.writeTo("players.mapping.players_unified_fbref") \
        .using("iceberg") \
        .createOrReplace()

    print("Tabla guardada en players.mapping.players_unified_fbref")
    return resultado

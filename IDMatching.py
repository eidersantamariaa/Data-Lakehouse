import pandas as pd
from funciones_mapeo import generar_clave
from ingesta import get_spark
"""
# 1. Cargar los DataFrames (de tus APIs)
df_api1 = ...
df_api2 = ...

# 2. Generar id_propio en API1
df_api1['id_propio'] = df_api1.apply(
    lambda r: generar_clave(r['nombre'], r['fecha_nacimiento']), axis=1
)

# 3. Construir tabla de mapeo aprovechando el enlace directo
df_mapeo = (
    df_api2[['idPlayer', 'col_con_id_api1']]
    .merge(
        df_api1[['id', 'id_propio']],
        left_on='col_con_id_api1',
        right_on='id',
        how='left'
    )
    .drop(columns='id')
    .rename(columns={
        'col_con_id_api1': 'id_api1',
        'idPlayer': 'id_api2'
    })
)

print(df_mapeo)
"""

def run(df1, df2):
    # 1. Generar clave única en ambos DataFrames
    df1['clave'] = df1.apply(lambda r: generar_clave(r['name'], r['dateOfBirth']), axis=1)

    # 2. Crear tabla de mapeo usando la clave
    mapeo = (
        df1[['idTransferMkt', 'clave']]
        .merge(
            df2[['id_api2', 'clave']],
            on='clave',
            how='left'
        )
        .drop(columns='clave')
    )

    return mapeo


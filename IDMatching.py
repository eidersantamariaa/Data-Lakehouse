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
    # 1. Generar clave única usando los campos reales de df1 (transfermarkt)
    df1['clave'] = df1.apply(
        lambda r: generar_clave(r['name'], r['dateOfBirth']), axis=1
    )

    # 2. Mapeo directo usando idTransferMkt que ya existe en df2
    mapeo = (
        df2[['idPlayer', 'idTransferMkt']]
        .merge(
            df1[['id', 'clave']],
            left_on='idTransferMkt',
            right_on='id',
            how='left'
        )
        .drop(columns='id')
        .rename(columns={
            'clave':          'id_propio',
            'idTransferMkt':  'id_transfermarkt',
            'idPlayer':       'id_thesportsdb'
        })
    )

    return mapeo
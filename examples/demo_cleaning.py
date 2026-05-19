import pandas as pd

from limpieza import (
    normalize_text,
    normalize_height,
    normalize_weight,
    normalize_currency,
    normalize_date,
    normalize_position,
)
from plata import detectar_solapamientos_agrupados, limpiar_tabla, validar_tabla


def make_example_df():
    return pd.DataFrame([
        {
            "id": 1,
            "tm_name": "Juan Perez",
            "ts_strPlayer": "Juan Perez",
            "fb_player": "J. Pérez",
            "tm_height": "1.80 m",
            "ts_height": "180",
            "tm_value": "€1.200.000",
            "ts_value": "$1,000,000",
            "age": "29",
            "some_date": "1992-05-01",
            "misc": {"name": "Napoli", "id": "6195"},
        },
        {
            "id": 2,
            "tm_name": "María López",
            "ts_strPlayer": None,
            "fb_player": "M. Lopez",
            "tm_height": "170cm",
            "ts_height": "1.70",
            "tm_value": "£800k",
            "ts_value": "800000",
            "age": "200",
            "some_date": "01/13/1988",  # invalid month/day order
            "misc": ["Spain", "Argentina"],
        },
        {
            "id": None,
            "tm_name": "  Carlos   ",
            "ts_strPlayer": "Carlos",
            "fb_player": None,
            "tm_height": "6 ft 2 in",
            "ts_height": "188",
            "tm_value": "not a price",
            "ts_value": None,
            "age": "25",
            "some_date": "1999.12.31",
            "misc": None,
        },
        # duplicate id 1 to test dedup
        {
            "id": 1,
            "tm_name": "Juan Perez",
            "ts_strPlayer": "Juan P.",
            "fb_player": "Juan Perez",
            "tm_height": "180 cm",
            "ts_height": "1.80",
            "tm_value": "1.2m",
            "ts_value": "£1,000,000",
            "age": "29",
            "some_date": "1992-05-01",
            "misc": {"club": "Napoli"},
        },
    ])


def main():
    df = make_example_df()
    print("\n=== ORIGINAL ===")
    print(df.to_string(index=False))

    # Detectar solapamientos y mostrar grupos
    grupos = detectar_solapamientos_agrupados(df, umbral_similitud=0.5)
    print("\n=== GRUPOS DETECTADOS ===")
    for g in grupos:
        print(g)

    # Limpiar tabla (consolidar columnas solapadas / votar)
    df_limpia = limpiar_tabla(df, grupos=grupos)
    print("\n=== DESPUÉS DE limpiar_tabla ===")
    print(df_limpia.to_string(index=False))

    # Validar tabla: registra errores y transforma valores inválidos a None
    df_validada, errores = validar_tabla(df_limpia)
    print("\n=== DESPUÉS DE validar_tabla ===")
    print(df_validada.to_string(index=False))
    print("\n=== ERRORES REGISTRADOS ===")
    print(errores.to_string(index=False))


if __name__ == "__main__":
    main()

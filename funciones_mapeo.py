import pandas as pd
from rapidfuzz import process, fuzz
import unicodedata
import re
from ingesta import get_spark

# ── 1. FUNCIONES DE NORMALIZACIÓN ──────────────────────────────────────────

def quitar_tildes(texto: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )

def extraer_inicial_apellido(nombre_raw: str) -> tuple[str, str]:
    """
    Siempre devuelve inicial + PRIMER apellido:
      - 'Oihan Sancet'              → ('O', 'Sancet')
      - 'Oihan Sancet Tirapu'       → ('O', 'Sancet')  ← ignora 2º apellido
      - 'Sancet, Oihan'             → ('O', 'Sancet')
      - 'Sancet Tirapu, Oihan'      → ('O', 'Sancet')  ← ignora 2º apellido
    """
    nombre_raw = quitar_tildes(nombre_raw.strip())

    # Formato "Apellido1 [Apellido2], Nombre"
    if ',' in nombre_raw:
        apellidos, nombre = nombre_raw.split(',', 1)
        inicial = nombre.strip()[0].upper()
        primer_apellido = apellidos.strip().split()[0].capitalize()  # ← solo el primero
        return inicial, primer_apellido

    # Formato "Nombre Apellido1 [Apellido2]"
    partes = nombre_raw.split()
    inicial = partes[0][0].upper()

    if len(partes) == 2:
        # Solo un apellido
        primer_apellido = partes[1].capitalize()
    else:
        # 2 o más apellidos → coge el del medio (posición -2 si hay 3 tokens)
        primer_apellido = partes[1].capitalize()  # siempre el segundo token

    return inicial, primer_apellido

def normalize_date(value):
    if value is None:
        return None
    v = str(value).strip()
    # detectar separador
    if "/" in v:
        sep = "/"
    elif "-" in v:
        sep = "-"
    elif "." in v:
        sep = "."
    else:
        return None
    
    parts = v.split(sep)
    if len(parts) != 3:
        return None
    
    if len(parts[0]) == 4:  # formato YYYY/MM/DD
        year, month, day = parts
    elif len(parts[2]) == 4:  # formato DD/MM/YYYY
        day, month, year = parts
    else:
        return None
    
    if not (year.isdigit() and month.isdigit() and day.isdigit()):
        return None
    
    if not (1 <= int(month) <= 12 and 1 <= int(day) <= 31):
        return None

    return f"{day.zfill(2)}{month.zfill(2)}{year}"

def generar_clave(nombre_raw: str, fecha_raw: str) -> str:
    inicial, apellido = extraer_inicial_apellido(nombre_raw)
    fecha = normalize_date(fecha_raw)
    return f"{inicial}{apellido}{fecha}"  # → OSancet25042000


# ── 2. GENERAR CLAVE EN CADA DATAFRAME ────────────────────────────────────

# Ajusta los nombres de columna a los tuyos
df_api1['clave'] = df_api1.apply(
    lambda r: generar_clave(r['nombre_completo'], r['fecha_nacimiento']), axis=1
)

df_api2['clave'] = df_api2.apply(
    lambda r: generar_clave(r['full_name'], r['birthdate']), axis=1
)


# ── 3. FUZZY MATCHING ENTRE CLAVES ────────────────────────────────────────

claves_api2 = df_api2['clave'].tolist()

def buscar_mejor_match(clave_api1: str, umbral: int = 85) -> str | None:
    resultado = process.extractOne(
        clave_api1,
        claves_api2,
        scorer=fuzz.ratio
    )
    if resultado and resultado[1] >= umbral:
        return resultado[0]
    return None  # Sin match suficientemente bueno

df_api1['clave_matched'] = df_api1['clave'].apply(buscar_mejor_match)


# ── 4. TABLA DE MAPEO FINAL ───────────────────────────────────────────────

mapeo = (
    df_api1[['id_api1', 'clave', 'clave_matched']]
    .merge(
        df_api2[['id_api2', 'clave']].rename(columns={'clave': 'clave_matched'}),
        on='clave_matched',
        how='left'
    )
    .rename(columns={'clave': 'id_propio'})
)

# Resultado:
#   id_propio        | id_api1 | id_api2
#   OSancet25042000  |   101   |  9981
#   LPerez14091998   |   102   |  9982

print(mapeo[['id_propio', 'id_api1', 'id_api2']])
print(f"\nSin match: {mapeo['id_api2'].isna().sum()} jugadores")

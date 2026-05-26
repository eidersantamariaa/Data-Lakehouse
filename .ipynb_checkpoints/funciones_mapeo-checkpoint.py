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
    if len(partes) == 1:
        return partes[0][0].upper(), partes[0].capitalize()
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

NATIONALITY_TO_CODE = {
    "Albania": "ALB",
    "Algeria": "ALG",
    "Andorra": "AND",
    "Angola": "ANG",
    "Argentina": "ARG",
    "Armenia": "ARM",
    "Australia": "AUS",
    "Austria": "AUT",
    "Bangladesh": "BAN",
    "Burundi": "BDI",
    "Belgium": "BEL",
    "Benin": "BEN",
    "Burkina Faso": "BFA",
    "Bosnia and Herzegovina": "BIH",
    "Bosnia-Herzegovina": "BIH",  # formato transfermarkt
    "Brazil": "BRA",
    "Barbados": "BRB",
    "Bulgaria": "BUL",
    "Canada": "CAN",
    "Congo": "CGO",
    "Chad": "CHA",
    "Chile": "CHI",
    "China": "CHN",
    "Ivory Coast": "CIV",
    "Cameroon": "CMR",
    "DR Congo": "COD",
    "Colombia": "COL",
    "Comoros": "COM",
    "Cape Verde": "CPV",
    "Costa Rica": "CRC",
    "Croatia": "CRO",
    "Central African Republic": "CTA",
    "Cuba": "CUB",
    "Curaçao": "CUW",
    "Cyprus": "CYP",
    "Czech Republic": "CZE",
    "Czechia": "CZE",
    "Denmark": "DEN",
    "Dominican Republic": "DOM",
    "Ecuador": "ECU",
    "Egypt": "EGY",
    "England": "ENG",
    "Equatorial Guinea": "EQG",
    "Spain": "ESP",
    "Estonia": "EST",
    "Finland": "FIN",
    "France": "FRA",
    "Faroe Islands": "FRO",
    "Gabon": "GAB",
    "Gambia": "GAM",
    "Georgia": "GEO",
    "Germany": "GER",
    "Ghana": "GHA",
    "Guadeloupe": "GLP",
    "Guinea-Bissau": "GNB",
    "Greece": "GRE",
    "Grenada": "GRN",
    "French Guiana": "GUF",
    "Guinea": "GUI",
    "Haiti": "HAI",
    "Honduras": "HON",
    "Hungary": "HUN",
    "Indonesia": "IDN",
    "Republic of Ireland": "IRL",
    "Ireland": "IRL",
    "Iran": "IRN",
    "Iraq": "IRQ",
    "Iceland": "ISL",
    "Israel": "ISR",
    "Italy": "ITA",
    "Jamaica": "JAM",
    "Jordan": "JOR",
    "Japan": "JPN",
    "Kenya": "KEN",
    "South Korea": "KOR",
    "Korea, South": "KOR",
    "Saudi Arabia": "KSA",
    "Kosovo": "KVX",
    "Liberia": "LBR",
    "Libya": "LBY",
    "Lithuania": "LTU",
    "Luxembourg": "LUX",
    "Latvia": "LVA",
    "Madagascar": "MAD",
    "Morocco": "MAR",
    "Malaysia": "MAS",
    "Moldova": "MDA",
    "Mexico": "MEX",
    "North Macedonia": "MKD",
    "Mali": "MLI",
    "Malta": "MLT",
    "Montenegro": "MNE",
    "Mozambique": "MOZ",
    "Montserrat": "MSR",
    "Mauritania": "MTN",
    "Martinique": "MTQ",
    "New Caledonia": "NCL",
    "Netherlands": "NED",
    "The Netherlands": "NED",
    "Nigeria": "NGA",
    "Niger": "NIG",
    "Northern Ireland": "NIR",
    "Norway": "NOR",
    "New Zealand": "NZL",
    "Panama": "PAN",
    "Paraguay": "PAR",
    "Peru": "PER",
    "Philippines": "PHI",
    "Palestine": "PLE",
    "Poland": "POL",
    "Portugal": "POR",
    "Puerto Rico": "PUR",
    "Romania": "ROU",
    "South Africa": "RSA",
    "Russia": "RUS",
    "Scotland": "SCO",
    "Senegal": "SEN",
    "Saint Kitts and Nevis": "SKN",
    "Sierra Leone": "SLE",
    "San Marino": "SMN",
    "Serbia": "SRB",
    "Switzerland": "SUI",
    "Suriname": "SUR",
    "Slovakia": "SVK",
    "Slovenia": "SVN",
    "Sweden": "SWE",
    "Syria": "SYR",
    "Tanzania": "TAN",
    "Thailand": "THA",
    "Togo": "TOG",
    "Trinidad and Tobago": "TRI",
    "Tunisia": "TUN",
    "Turkey": "TUR",
    "United Arab Emirates": "UAE",
    "Uganda": "UGA",
    "Ukraine": "UKR",
    "Uruguay": "URU",
    "United States": "USA",
    "Uzbekistan": "UZB",
    "Venezuela": "VEN",
    "Wales": "WAL",
    "Zambia": "ZAM",
    "Zimbabwe": "ZIM",
}

def normalizar_nacion(nombre):
    return NATIONALITY_TO_CODE.get(nombre)

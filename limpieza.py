from pyspark.sql.functions import col, trim, udf
from pyspark.sql.types import StringType, FloatType
from ingesta import get_spark
import re
import unicodedata

def clean_basic(df, primary_key=None):
    # Si no se pasa primary_key, busca automáticamente
    if primary_key is None:
        candidates = [c for c in df.columns if c.lower().startswith("id")]
        primary_key = candidates[0] if candidates else None

    for col_name in df.columns:
        stripped = col_name.strip()
        if stripped != col_name:
            df = df.withColumnRenamed(col_name, stripped)

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))

    if primary_key:
        df = df.dropna(subset=[primary_key])
        df = df.dropDuplicates([primary_key])
    else:
        df = df.dropDuplicates()
    return df

def run_silver(config):
    spark = get_spark()

    for table_name, transform_fn in config.SILVER_TRANSFORMS.items():
        bronze_table = f"players.{config.NAMESPACE}.players_united"
        silver_table = f"players.{config.NAMESPACE}.{table_name}_plata"

        print(f"Transforming {table_name} Bronze → Silver...")
        df_bronze = spark.table(bronze_table)
        df_silver = transform_fn(df_bronze)
        df_silver.writeTo(silver_table).createOrReplace()
        print(f"✅ {silver_table}: {df_silver.count()} rows")

# ------------------------------- Normalización -------------------------------

def normalize_text(name):
    if name is None:
        return ""
    name = name.lower().strip()
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()
    return name

def normalize_height(value):
    v = str(value).lower()
    v = v.replace(",", ".")
    v = v.replace("'", "ft")
    v = v.replace("\"", "in")
    if "cm" in v:
        return float(re.findall(r"\d+\.?\d*", v)[0])
    
    if "ft" in v:
        ft = re.findall(r"(\d+)\s*ft", v)
        inch = re.findall(r"(\d+)\s*in", v)
        ft = float(ft[0]) if ft else 0
        inch = float(inch[0]) if inch else 0
        return float((ft * 30.48) + (inch * 2.54))
    
    if "m" in v:
        return float(float(re.findall(r"\d+\.?\d*", v)[0]) * 100)
    
    if re.fullmatch(r"\d+\.?\d*", v.strip()):
        n = float(re.findall(r"\d+\.?\d*", v)[0])
        if n >= 100:      # ya está en cm (ej: 179, 180)
            return n
        else:             # está en metros (ej: 1.79, 1.80)
            return n * 100
    return None
"""
test_cases = [
    # (input, descripción)
    ("1.82 m (6 ft 0 in)",   "metros con pies al lado"),
    ("1.75m",                 "metros sin espacio"),
    ("175 cm",                "centímetros entero"),
    ("175.5 cm",              "centímetros decimal"),
    ("6 ft 2 in",             "pies y pulgadas"),
    ("6 ft",                  "solo pies"),
    ("6'2\"",                "formato anglosajón con comillas"),
    ("196",                   "número limpio entero"),
    ("1.96",                  "número limpio decimal"),
    ("1,82 m",                "metros con coma"),
    ("180CM",                 "cm en mayúsculas"),
    ("2 m",                   "metros entero"),
]

for value, desc in test_cases:
    result = normalize_height(value)
    print(f"{str(value):<25} → {str(result):<10}  ({desc})")
"""

def normalize_weight(value):
    v = str(value).lower()
    v = v.replace(",", ".")
    if "kg" in v:
        if "." in v:
            return float(re.findall(r"\d+\.?\d*", v)[0])
        else:
            return float(re.findall(r"\d+", v)[0])
    if "lb" in v:
        if "." in v:
            lbs = float(re.findall(r"\d+\.?\d*", v)[0])
            return float(f"{lbs * 0.453592:.2f}")
        else:
            lbs = float(re.findall(r"\d+", v)[0])
            return float(f"{lbs * 0.453592:.2f}")
    return None

def normalize_currency(value):
    if value is None:
        return None
    v = str(value).lower().strip()
    v = v.replace("gbp", "£").replace("usd", "$").replace("eur", "€")
    if "£" in v:
        currency = "£"
    elif "$" in v:
        currency = "$"
    elif "€" in v:
        currency = "€"
    else:
        # sin símbolo — si hay números simplifica, si no devuelve el texto
        nums = re.findall(r"-?\d+\.?\d*", v)
        if not nums:
            return str(value).strip()
        amount = float(nums[0])
        if amount < 0:
            return str(value).strip()  # mantiene "-" o texto con minus
        if amount >= 1_000_000:
            return f"{round(amount / 1_000_000, 2)}m"
        elif amount >= 1_000:
            return f"{round(amount / 1_000, 2)}k"
        else:
            return str(amount)
    
    num_str = v.replace(currency, "").strip()
    s = num_str.strip()

    multiplier = 1
    if "million" in s or "mill" in s or "millones" in s:
        multiplier = 1_000_000
        s = s[:s.rfind("m")].strip()  # quita todo desde la m de mill
    elif "miles" in s or "mil" in s:
        multiplier = 1_000
        s = re.sub(r"mil(es)?$", "", s).strip()
    elif s.endswith("m"):
        multiplier = 1_000_000
        s = s[:-1].strip()
    elif s.endswith("k"):
        multiplier = 1_000
        s = s[:-1].strip()

    has_comma = "," in s
    has_dot   = "." in s

    if has_comma and has_dot:
        # el último separador es el decimal
        # 1,000.50 → quita comas   |   1.000,50 → quita puntos y coma→punto
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")           # 1,000.50 → 1000.50
        else:
            s = s.replace(".", "").replace(",", ".")  # 1.000,50 → 1000.50

    elif has_comma:
        after_comma = s.split(",")[-1]
        if len(after_comma) == 3:
            s = s.replace(",", "")           # 1,000 → 1000 (miles)
        else:
            s = s.replace(",", ".")          # 1,5 → 1.5 (decimal)

    elif has_dot:
        after_comma = s.split(".")[-1]
        if len(after_comma) == 3:
            s = s.replace(".", "")           # 1.000 → 1000 (miles)

    nums = re.findall(r"\d+\.?\d*", s)
    if not nums:
        return str(value).strip()
    s = nums[0]

    amount = float(s) * multiplier
    # convertir a EUR
    if currency == "£":
        amount = round(amount * 1.15, 2)
    if currency == "$":
        amount = round(amount * 0.86, 2)
    if currency == "€":
        amount = round(amount, 2)

    # formatear como xk o xm
    if amount >= 1_000_000:
        return f"{round(amount / 1_000_000, 2)}m"
    elif amount >= 1_000:
        return f"{round(amount / 1_000, 2)}k"
    else:
        return str(amount)

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

    return f"{day.zfill(2)}-{month.zfill(2)}-{year}"


normalize_text_udf     = udf(normalize_text, StringType())
normalize_height_udf   = udf(normalize_height, FloatType())
normalize_weight_udf   = udf(normalize_weight, FloatType())
normalize_currency_udf = udf(normalize_currency, StringType())
normalize_date_udf     = udf(normalize_date, StringType())
# limpieza.py
import pandas as pd
import re
from datetime import datetime
from rapidfuzz import fuzz

from limpieza import normalize_text, normalize_date, normalize_height, normalize_weight, normalize_currency, normalize_position

errores_log = []

# ── Detección automática ─────────────────────────────────────────────────────
def similitud_fuzzy(s1, s2):
    return fuzz.ratio(str(s1).lower().strip(), str(s2).lower().strip()) / 100

def detectar_solapamientos(df, umbral_similitud=0.7):
    from itertools import combinations

    tm_cols = [c for c in df.columns if c.startswith("tm_")]
    ts_cols = [c for c in df.columns if c.startswith("ts_")]
    fb_cols = [c for c in df.columns if c.startswith("fb_")]

    candidatos = []

    for col1, col2 in combinations(tm_cols + ts_cols + fb_cols, 2):
        prefix1 = col1.split("_")[0]
        prefix2 = col2.split("_")[0]
        if prefix1 == prefix2:
            continue

        ambas = df[[col1, col2]].dropna()
        if len(ambas) < 10:
            continue

        try:
            s1 = ambas[col1].astype(str).str.lower().str.strip()
            s2 = ambas[col2].astype(str).str.lower().str.strip()

            # Comparación fuzzy fila a fila
            scores = [fuzz.ratio(a, b) / 100 for a, b in zip(s1, s2)]
            coincidencias = sum(s >= umbral_similitud for s in scores) / len(scores)

            if coincidencias >= 0.5:  # al menos 50% de filas son similares
                candidatos.append({
                    "col1": col1,
                    "col2": col2,
                    "coincidencia": round(coincidencias, 2)
                })
        except:
            continue

    return pd.DataFrame(candidatos).sort_values("coincidencia", ascending=False)

def configurar_solapamientos(df, umbral_similitud=0.5):
    """
    Detecta solapamientos y pregunta al usuario qué columnas quiere consolidar.
    Devuelve un COLUMNAS_CONFIG listo para usar en limpiar_tabla.
    """
    candidatos = detectar_solapamientos(df, umbral_similitud)
    
    if candidatos.empty:
        print("No se detectaron solapamientos.")
        return []

    config = []
    grupos_procesados = set()

    for _, row in candidatos.iterrows():
        col1, col2 = row["col1"], row["col2"]

        # Evitar procesar columnas ya agrupadas
        if col1 in grupos_procesados or col2 in grupos_procesados:
            continue

        print(f"\n{'='*50}")
        print(f"Posible solapamiento ({row['coincidencia']*100:.0f}% coincidencia):")
        print(f"  1: {col1}")
        print(f"  2: {col2}")
        print(f"Ejemplo de valores:")
        muestra = df[[col1, col2]].dropna().head(3)
        print(muestra.to_string(index=False))

        respuesta = input("\n¿Consolidar estas columnas? (s/n): ").strip().lower()
        if respuesta != "s":
            continue

        # Buscar si hay más columnas relacionadas
        relacionadas = candidatos[
            (candidatos["col1"].isin([col1, col2])) |
            (candidatos["col2"].isin([col1, col2]))
        ]
        todas_cols = set([col1, col2])
        for _, r in relacionadas.iterrows():
            todas_cols.add(r["col1"])
            todas_cols.add(r["col2"])
        todas_cols = list(todas_cols)

        if len(todas_cols) > 2:
            print(f"\nTambién hay columnas relacionadas: {todas_cols}")
            print("¿Cuáles incluir? (escribe los nombres separados por coma, o Enter para todas):")
            seleccion = input().strip()
            if seleccion:
                todas_cols = [c.strip() for c in seleccion.split(",")]

        col_final = input(f"\nNombre de la columna consolidada: ").strip()
        
        prefijos = [c.split("_")[0] for c in todas_cols]
        tiebreaker = todas_cols[prefijos.index("tm")] if "tm" in prefijos else todas_cols[0]
        print(f"Tiebreaker por defecto: {tiebreaker} (Enter para confirmar o escribe otro):")
        tb = input().strip()
        if tb:
            tiebreaker = tb

        config.append({
            "col_final":  col_final,
            "fuentes":    todas_cols,
            "tiebreaker": tiebreaker,
            "normalizar": lambda x: x,  # sin normalización por defecto
        })

        grupos_procesados.update(todas_cols)
        print(f"✓ Añadido: {col_final} ← {todas_cols}")

    print(f"\n{'='*50}")
    print(f"Config generado con {len(config)} columnas consolidadas.")
    print("Puedes añadir funciones de normalización manualmente en COLUMNAS_CONFIG.")
    
    return config

def _registrar_error(col, valor, motivo, idx):
    errores_log.append({
        "columna": col,
        "valor":   valor,
        "motivo":  motivo,
        "fila":    idx
    })

def from_list(normalizar):
    """Wrapper para columnas que pueden ser lista o string."""
    def _inner(v):
        if isinstance(v, list):
            v = v[0] if len(v) > 0 else None
        return normalizar(v) if v is not None else None
    return _inner

def extraer_nombre_equipo(v):
    if not isinstance(v, list):
        return normalize_text(v) if v else None
    
    for item in v:
        if item is None:
            continue
        # Descartar fechas (YYYY-MM-DD) y números puros
        if re.match(r'^\d{4}-\d{2}-\d{2}$', str(item)):
            continue
        if re.match(r'^\d+$', str(item)):
            continue
        return normalize_text(item)
    
    return None

# ── Funciones de validación ──────────────────────────────────────────────────

def validar_edad(v):
    try:
        n = int(float(str(v).split("-")[0]))
        if n < 10 or n > 50:
            return None, f"Edad fuera de rango: {v}"
        return n, None
    except:
        return None, f"Edad no parseable: {v}"

def validar_fecha(v):
    if v is None:
        return None, None
    formatos = ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]
    for fmt in formatos:
        try:
            d = datetime.strptime(str(v)[:10], fmt)
            if not (1 <= d.month <= 12):
                return None, f"Mes inválido: {v}"
            if not (1 <= d.day <= 31):
                return None, f"Día inválido: {v}"
            if d.year < 1940 or d.year > 2015:
                return None, f"Año fuera de rango para jugador: {v}"
            return str(v), None
        except:
            continue
    return None, f"Formato de fecha no reconocido: {v}"

def validar_altura(v):
    try:
        n = float(v)
        if n < 140 or n > 220:
            return None, f"Altura fuera de rango (cm): {v}"
        return n, None
    except:
        return None, f"Altura no parseable: {v}"

def validar_url(v):
    if v and not str(v).startswith("http"):
        return None, f"URL inválida: {v}"
    return v, None

def validar_año(v):
    try:
        n = int(float(str(v)))
        if n < 1940 or n > 2015:
            return None, f"Año de nacimiento fuera de rango: {v}"
        return n, None
    except:
        return None, f"Año no parseable: {v}"

# ── Config validaciones ──────────────────────────────────────────────────────

VALIDACIONES = [
    {"col": "fb_age",      "validar": validar_edad},
    {"col": "born",        "validar": validar_fecha},
    {"col": "tm_dateOfBirth", "validar": validar_fecha},
    {"col": "ts_dateBorn", "validar": validar_fecha},
    {"col": "height",      "validar": validar_altura},
    {"col": "tm_imageUrl", "validar": validar_url},
    {"col": "fb_born",     "validar": validar_año},
]

COLUMNAS_CONFIG = [
    {
        "col_final":  "name",
        "fuentes":    ["tm_name", "ts_strPlayer", "fb_player"],
        "tiebreaker": "tm_name",
        "normalizar": normalize_text,
    },
    {
        "col_final":  "born",
        "fuentes":    ["tm_dateOfBirth", "ts_dateBorn", "fb_born"],
        "tiebreaker": "tm_dateOfBirth",
        "normalizar": normalize_date,
    },
    {
        "col_final":  "nationality",
        "fuentes":    ["tm_citizenship", "ts_strNationality", "fb_nation"],
        "tiebreaker": "tm_citizenship",
        "normalizar": from_list(normalize_text),  # lista
    },
    {
        "col_final":  "position",
        "fuentes":    ["tm_position", "ts_strPosition", "fb_pos"],
        "tiebreaker": "tm_position",
        "normalizar": from_list(normalize_position),  # lista
    },
    {
        "col_final":  "height (cm)",
        "fuentes":    ["tm_height", "ts_strHeight"],
        "tiebreaker": "tm_height",
        "normalizar": normalize_height,
    },
    {
        "col_final":  "team",
        "fuentes":    ["tm_club", "ts_strTeam", "fb_team"],
        "tiebreaker": "tm_club",
        "normalizar": extraer_nombre_equipo,  # lista
    },
    {
        "col_final":  "weight (kg)",
        "fuentes":    ["ts_strWeight"],
        "tiebreaker": "ts_strWeight",
        "normalizar": normalize_weight,  # lista
    },
]

COLUMNAS_NORMALIZAR = [
    {"col": "tm_marketValue", "normalizar": normalize_currency},
    {"col": "fb_age", "normalizar": lambda v: int(str(v).split("-")[0].strip()) if v and "-" in str(v) else v},
]


def _votar(valores_norm):
    """
    Recibe dict {col: valor_normalizado}.
    Devuelve el valor en el que coinciden >=2 fuentes.
    Si todas difieren devuelve None (el caller pone el tiebreaker).
    """
    conteo = {}
    for v in valores_norm.values():
        if v is None:
            continue
        conteo[v] = conteo.get(v, 0) + 1

    mayoria = [v for v, n in conteo.items() if n >= 2]
    return mayoria[0] if mayoria else None

def validar_tabla(df, validaciones=VALIDACIONES):
    global errores_log
    errores_log = []
    df = df.copy()

    for entrada in validaciones:
        col     = entrada["col"]
        validar = entrada["validar"]

        if col not in df.columns:
            continue

        def aplicar(row, col=col, validar=validar):
            v = row[col]
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return v
            valor_limpio, error = validar(v)
            if error:
                _registrar_error(col, v, error, row.name)
            return valor_limpio

        df[col] = df.apply(aplicar, axis=1)

    # Resumen
    print(f"Total errores detectados: {len(errores_log)}")
    errores_por_col = pd.DataFrame(errores_log).groupby("columna").size() if errores_log else {}
    for col, n in errores_por_col.items():
        print(f"  {col}: {n} errores")

    return df, pd.DataFrame(errores_log)

def limpiar_tabla(df, config=COLUMNAS_CONFIG, config_norm=COLUMNAS_NORMALIZAR):
    """
    df: pandas DataFrame con columnas prefijadas (tm_, ts_, fb_...)
    config: lista de dicts con col_final, fuentes, tiebreaker, normalizar

    Para añadir una nueva columna consolidada basta con añadir
    un dict a COLUMNAS_CONFIG.
    """
    df = df.copy()
    cols_a_eliminar = []

    # Paso 1: votar columnas solapadas
    for entrada in config:
        col_final  = entrada["col_final"]
        fuentes    = [f for f in entrada["fuentes"] if f in df.columns]
        tiebreaker = entrada["tiebreaker"]
        normalizar = entrada.get("normalizar", lambda x: x)

        if not fuentes:
            continue

        def resolver_fila(row, fuentes=fuentes, tiebreaker=tiebreaker, normalizar=normalizar):
            # Normalizar cada valor
            valores_norm = {}
            for col in fuentes:
                v = row.get(col)
                if v is not None and v == v:  # filtrar None y NaN
                    try:
                        valores_norm[col] = normalizar(v)
                    except:
                        valores_norm[col] = None

            # Votar
            ganador = _votar(valores_norm)
            if ganador is not None:
                return ganador

            # Tiebreaker
            return valores_norm.get(tiebreaker)

        df[col_final] = df.apply(resolver_fila, axis=1)
        cols_a_eliminar.extend(fuentes)

    df = df.drop(columns=cols_a_eliminar)

    # Paso 2: normalizar columnas únicas
    for entrada in config_norm:
        col = entrada["col"]
        if col not in df.columns:
            continue
        normalizar = entrada["normalizar"]
        df[col] = df[col].apply(lambda v: normalizar(v) if v is not None and v == v else None)

    # Paso 3: aplanar columnas que son listas
    def aplanar_lista(v):
        if isinstance(v, list):
            return ", ".join(str(i) for i in v if i is not None)
        return v

    for col in df.columns:
        if df[col].dropna().apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda v: aplanar_lista(v) if v is not None and v == v else None)

    # Reordenar: ids + cols consolidadas + resto
    cols_ids = [c for c in df.columns if c == "id"]
    cols_consolidadas = [e["col_final"] for e in config if e["col_final"] in df.columns]
    cols_resto       = [c for c in df.columns if c not in cols_ids and c not in cols_consolidadas]

    df = df[cols_ids + cols_consolidadas + cols_resto]

    print(f"Columnas consolidadas : {cols_consolidadas}")
    print(f"Total columnas        : {len(df.columns)}")
    print(f"Total filas           : {len(df)}")

    return df

import pandas as pd
import re
from datetime import datetime
from rapidfuzz import fuzz
from pyspark.sql import types as T

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
        if len(ambas) < 3:
            continue

        try:
            s1 = ambas[col1].astype(str).str.lower().str.strip()
            s2 = ambas[col2].astype(str).str.lower().str.strip()

            scores = [fuzz.ratio(a, b) / 100 for a, b in zip(s1, s2)]
            coincidencias = sum(s >= umbral_similitud for s in scores) / len(scores)

            if coincidencias >= 0.5:
                candidatos.append({
                    "col1": col1,
                    "col2": col2,
                    "coincidencia": round(coincidencias, 2)
                })
        except:
            continue

    if not candidatos:
        return pd.DataFrame(columns=["col1", "col2", "coincidencia"])

    return pd.DataFrame(candidatos).sort_values("coincidencia", ascending=False)

def detectar_solapamientos_agrupados(df, umbral_similitud=0.5):
    """
    Agrupa los pares solapados en clusters de columnas relacionadas.
    Ej: si tm_name↔ts_strPlayer y tm_name↔fb_player → grupo [tm_name, ts_strPlayer, fb_player]
    """
    candidatos = detectar_solapamientos(df, umbral_similitud)
    if candidatos.empty:
        return []

    parent = {}

    def find(x):
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x, y):
        parent[find(x)] = find(y)

    for _, row in candidatos.iterrows():
        union(row["col1"], row["col2"])

    grupos = {}
    for col in set(candidatos["col1"]) | set(candidatos["col2"]):
        root = find(col)
        grupos.setdefault(root, set()).add(col)

    resultado = []
    for cols in grupos.values():
        cols = list(cols)
        scores = candidatos[
            candidatos["col1"].isin(cols) & candidatos["col2"].isin(cols)
        ]["coincidencia"]
        resultado.append({
            "columnas": cols,
            "coincidencia": round(scores.mean(), 2) if len(scores) else 0
        })

    return sorted(resultado, key=lambda x: x["coincidencia"], reverse=True)

def configurar_solapamientos(df, umbral_similitud=0.5):
    """
    Detecta solapamientos y pregunta al usuario qué columnas quiere consolidar.
    Devuelve una config lista para pasar directamente a limpiar_tabla().
    """
    candidatos = detectar_solapamientos(df, umbral_similitud)

    if candidatos.empty:
        print("No se detectaron solapamientos.")
        return []

    config = []
    grupos_procesados = set()

    for _, row in candidatos.iterrows():
        col1, col2 = row["col1"], row["col2"]

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
            "normalizar": lambda x: x,
        })

        grupos_procesados.update(todas_cols)
        print(f"✓ Añadido: {col_final} ← {todas_cols}")

    print(f"\n{'='*50}")
    print(f"Config generado con {len(config)} columnas consolidadas.")
    print("Puedes pasar el resultado directamente a limpiar_tabla().")

    return config

def _registrar_error(col, valor, motivo, idx):
    errores_log.append({
        "columna": col,
        "valor":   valor,
        "motivo":  motivo,
        "fila":    idx
    })

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
            if d.year < 1940 or d.year > 2050:
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

def _inferir_validador(serie):
    muestra = serie.dropna().astype(str)
    if muestra.str.fullmatch(r"\d{4}-\d{2}-\d{2}.*").mean() > 0.5:
        return validar_fecha
    if muestra.str.fullmatch(r"\d{1,2}").mean() > 0.5:
        return validar_edad
    if muestra.str.match(r"https?://").mean() > 0.5:
        return validar_url
    if muestra.str.fullmatch(r"\d{3}").mean() > 0.5:
        return validar_altura
    return None

# ── Helpers internos ─────────────────────────────────────────────────────────

def _es_nulo(v):
    if v is None:
        return True
    if isinstance(v, (list, tuple, dict, set)):
        return False
    try:
        na = pd.isna(v)
        return bool(na) if isinstance(na, bool) else False
    except Exception:
        return False

def _clave_voto(v):
    if isinstance(v, dict):
        return tuple(sorted((k, _clave_voto(val)) for k, val in v.items()))
    if isinstance(v, (list, tuple, set)):
        return tuple(_clave_voto(x) for x in v)
    if hasattr(v, "tolist"):
        return _clave_voto(v.tolist())
    return v

def _votar(valores_norm):
    """
    Recibe dict {col: valor_normalizado}.
    Devuelve el valor en el que coinciden >=2 fuentes.
    Si todas difieren devuelve None (el caller pone el tiebreaker).
    """
    conteo = {}
    originales = {}
    for v in valores_norm.values():
        if _es_nulo(v):
            continue
        clave = _clave_voto(v)
        conteo[clave] = conteo.get(clave, 0) + 1
        if clave not in originales:
            originales[clave] = v

    mayoria = [k for k, n in conteo.items() if n >= 2]
    return originales[mayoria[0]] if mayoria else None

# ── Validación ───────────────────────────────────────────────────────────────

def validar_tabla(df):
    """
    Recorre todas las columnas, infiere el validador apropiado por contenido
    y registra/corrige los valores fuera de rango.
    """
    global errores_log
    errores_log = []
    df = df.copy()

    for col in df.columns:
        validar = _inferir_validador(df[col])
        if validar is None:
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

    print(f"Total errores detectados: {len(errores_log)}")
    errores_por_col = pd.DataFrame(errores_log).groupby("columna").size() if errores_log else {}
    for col, n in errores_por_col.items():
        print(f"  {col}: {n} errores")

    return df, pd.DataFrame(errores_log)

# ── Limpieza principal ───────────────────────────────────────────────────────

def limpiar_tabla(df, grupos=None):
    """
    Consolida columnas solapadas entre fuentes (tm_, ts_, fb_...) en una
    sola columna por concepto, votando entre fuentes cuando hay mayoría
    y usando el tiebreaker (preferencia tm_) en caso de empate.

    grupos: output de detectar_solapamientos_agrupados() o configurar_solapamientos().
            Si es None se detectan automáticamente.
    """
    if grupos is None:
        grupos = detectar_solapamientos_agrupados(df)

    # Convertir formato grupos/config → config interna
    # Soporta dos formatos:
    # - Deteccion automatica: {"columnas": [...], "coincidencia": ...}
    # - Config manual UI: {"col_final": ..., "fuentes": [...], "tiebreaker": ...}
    config = []
    for grupo in grupos:
        if isinstance(grupo, dict) and "fuentes" in grupo:
            cols = [c for c in grupo.get("fuentes", []) if isinstance(c, str)]
            if not cols:
                continue
            prefijos = [c.split("_")[0] for c in cols]
            tiebreaker = grupo.get("tiebreaker")
            if tiebreaker not in cols:
                tiebreaker = cols[prefijos.index("tm")] if "tm" in prefijos else cols[0]
            col_final = str(grupo.get("col_final") or "").strip()
            if not col_final:
                col_final = "_".join(tiebreaker.split("_")[1:])  # tm_dateOfBirth → dateOfBirth
        else:
            cols = grupo["columnas"]
            prefijos = [c.split("_")[0] for c in cols]
            tiebreaker = cols[prefijos.index("tm")] if "tm" in prefijos else cols[0]
            col_final = "_".join(tiebreaker.split("_")[1:])  # tm_dateOfBirth → dateOfBirth
        config.append({
            "col_final":  col_final,
            "fuentes":    cols,
            "tiebreaker": tiebreaker,
            "normalizar": lambda x: x,
        })

    df = df.copy()
    cols_a_eliminar = []

    # Paso 1: votar columnas solapadas y consolidar
    for entrada in config:
        col_final  = entrada["col_final"]
        fuentes    = [f for f in entrada["fuentes"] if f in df.columns]
        tiebreaker = entrada["tiebreaker"]
        normalizar = entrada.get("normalizar", lambda x: x)

        if not fuentes:
            continue

        def resolver_fila(row, fuentes=fuentes, tiebreaker=tiebreaker, normalizar=normalizar):
            valores_norm = {}
            for col in fuentes:
                v = row.get(col)
                if not _es_nulo(v):
                    try:
                        valores_norm[col] = normalizar(v)
                    except:
                        valores_norm[col] = None

            ganador = _votar(valores_norm)
            if ganador is not None:
                return ganador

            return valores_norm.get(tiebreaker)

        df[col_final] = df.apply(resolver_fila, axis=1)
        cols_a_eliminar.extend(fuentes)

    df = df.drop(columns=cols_a_eliminar)

    # Paso 2: aplanar columnas con valores complejos para que Spark pueda inferir tipo
    def aplanar_valor(v):
        if isinstance(v, dict):
            return ", ".join(f"{k}={i}" for k, i in v.items() if i is not None)
        if isinstance(v, (list, tuple, set)):
            return ", ".join(str(i) for i in v if i is not None)
        if hasattr(v, "tolist") and not isinstance(v, (str, bytes)):
            try:
                return aplanar_valor(v.tolist())
            except Exception:
                return str(v)
        return v

    for col in df.columns:
        if df[col].dropna().apply(
            lambda x: isinstance(x, (list, tuple, set, dict))
            or (hasattr(x, "tolist") and not isinstance(x, (str, bytes)))
        ).any():
            df[col] = df[col].apply(lambda v: aplanar_valor(v) if not _es_nulo(v) else None)

    # Reordenar: id + cols consolidadas + resto
    cols_ids         = [c for c in df.columns if c == "id"]
    cols_consolidadas = [e["col_final"] for e in config if e["col_final"] in df.columns]
    cols_resto        = [c for c in df.columns if c not in cols_ids and c not in cols_consolidadas]

    df = df[cols_ids + cols_consolidadas + cols_resto]

    print(f"Columnas consolidadas : {cols_consolidadas}")
    print(f"Total columnas        : {len(df.columns)}")
    print(f"Total filas           : {len(df)}")

    return df

#-----------------------------------------De DataFrame a spark-----------------------------------------

def pandas_dtype_to_spark(dtype):

    if pd.api.types.is_integer_dtype(dtype):
        return T.LongType()

    elif pd.api.types.is_float_dtype(dtype):
        return T.DoubleType()

    elif pd.api.types.is_bool_dtype(dtype):
        return T.BooleanType()

    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return T.TimestampType()

    else:
        return T.StringType()


def pandas_to_spark(spark_session, df_pandas):

    df_fixed = df_pandas.copy()

    for col in df_fixed.columns:

        if df_fixed[col].dtype == object:

            df_fixed[col] = df_fixed[col].where(
                df_fixed[col].notna(),
                other=None
            )

            df_fixed[col] = df_fixed[col].apply(
                lambda x: str(x) if x is not None else None
            )

    fields = [
        T.StructField(
            col,
            pandas_dtype_to_spark(df_fixed[col].dtype),
            nullable=True
        )
        for col in df_fixed.columns
    ]

    schema = T.StructType(fields)

    return spark_session.createDataFrame(
        df_fixed,
        schema=schema
    )
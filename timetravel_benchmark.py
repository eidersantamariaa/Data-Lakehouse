"""
Iceberg Time Travel Benchmark — Spark + REST Catalog (Garage)
=============================================================
Compara el tiempo de consulta entre la versión actual de una tabla Iceberg
y versiones antiguas usando time travel (por snapshot o timestamp).

Uso:
    python iceberg_timetravel_benchmark.py \
        --database my_db \
        --table    my_table \
        --query    "SELECT COUNT(*) FROM {table}" \
        --runs     10
"""

import argparse
import statistics
import time
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import SparkSession

# Importa tu función de sesión — ajusta el path si es necesario
from ingesta import get_spark


# ─────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────

CATALOG_NAME     = "players"
DEFAULT_DATABASE = "transfermarkt"
DEFAULT_TABLE    = "players_bronce"
DEFAULT_QUERY    = "SELECT COUNT(*) FROM {table}"
DEFAULT_RUNS     = 10
WARMUP_RUNS      = 2   # ejecuciones de calentamiento (no se miden)


# ─────────────────────────────────────────────
# Helpers Iceberg
# ─────────────────────────────────────────────

def get_snapshots(spark: SparkSession, full_table: str) -> list[dict]:
    """
    Devuelve la lista de snapshots disponibles en la tabla,
    ordenados del más reciente al más antiguo.
    """
    df = spark.sql(
        f"SELECT snapshot_id, committed_at, operation "
        f"FROM {full_table}.snapshots "
        f"ORDER BY committed_at DESC"
    )
    return [row.asDict() for row in df.collect()]


def run_query(spark: SparkSession, sql: str) -> float:
    """Ejecuta una query SQL y devuelve el tiempo transcurrido en segundos."""
    t0 = time.perf_counter()
    spark.sql(sql).collect()
    return time.perf_counter() - t0


def build_timetravel_sql(sql: str,
                          snapshot_id: Optional[int],
                          timestamp: Optional[str]) -> str:
    """
    Inserta la cláusula de time travel en el SQL original.

    Ejemplos de salida:
        ... FROM players.db.tabla VERSION AS OF 8765432198765 ...
        ... FROM players.db.tabla TIMESTAMP AS OF '2024-01-15 12:00:00' ...
    """
    table_ref = _extract_table_ref(sql)
    if snapshot_id is not None:
        replacement = f"{table_ref} VERSION AS OF {snapshot_id}"
    else:
        replacement = f"{table_ref} TIMESTAMP AS OF '{timestamp}'"
    return sql.replace(table_ref, replacement, 1)


def _extract_table_ref(sql: str) -> str:
    """
    Extrae el token de tabla real después de FROM,
    ignorando los FROM seguidos de subqueries (paréntesis).
    """
    tokens = sql.split()
    for i, token in enumerate(tokens):
        if token.upper() == "FROM" and i + 1 < len(tokens):
            next_token = tokens[i + 1]
            if not next_token.startswith("("):
                return next_token
    raise ValueError(f"No se encontró referencia a tabla en: {sql}")


# ─────────────────────────────────────────────
# Benchmark
# ─────────────────────────────────────────────

@dataclass
class BenchmarkResult:
    label: str
    times: list[float] = field(default_factory=list)

    @property
    def mean(self)    -> float: return statistics.mean(self.times)
    @property
    def median(self)  -> float: return statistics.median(self.times)
    @property
    def stdev(self)   -> float: return statistics.stdev(self.times) if len(self.times) > 1 else 0.0
    @property
    def p95(self)     -> float: return sorted(self.times)[max(0, int(len(self.times) * 0.95) - 1)]
    @property
    def minimum(self) -> float: return min(self.times)
    @property
    def maximum(self) -> float: return max(self.times)


def run_benchmark(spark: SparkSession,
                  sql_current: str,
                  sql_timetravel: str,
                  n_runs: int) -> tuple[BenchmarkResult, BenchmarkResult]:
    """
    Ejecuta las dos queries de forma intercalada para minimizar
    el efecto de condiciones cambiantes del entorno.
    """
    current_result = BenchmarkResult(label="CURRENT  (sin time travel)")
    tt_result      = BenchmarkResult(label="TIME TRAVEL")

    print(f"\n🔥 Calentando ({WARMUP_RUNS} ejecuciones sin medir)…")
    for _ in range(WARMUP_RUNS):
        run_query(spark, sql_current)
        run_query(spark, sql_timetravel)

    print(f"⏱  Midiendo {n_runs} ejecuciones (intercaladas)…\n")
    for i in range(1, n_runs + 1):
        t_cur = run_query(spark, sql_current)
        current_result.times.append(t_cur)

        t_tt = run_query(spark, sql_timetravel)
        tt_result.times.append(t_tt)

        diff = t_tt - t_cur
        sign = "+" if diff >= 0 else ""
        print(f"  Run {i:02d}/{n_runs}  "
              f"current={t_cur:.3f}s  "
              f"timetravel={t_tt:.3f}s  "
              f"diff={sign}{diff:.3f}s")

    return current_result, tt_result


# ─────────────────────────────────────────────
# Reporte
# ─────────────────────────────────────────────

def print_report(results: list[BenchmarkResult], sql: str) -> None:
    w = 66
    print("\n" + "═" * w)
    print("  ICEBERG TIME TRAVEL BENCHMARK — RESULTADOS")
    print("═" * w)
    print(f"  Query: {sql}")
    print("─" * w)
    print(f"{'Métrica':<12}" + "".join(f"{r.label:>27}" for r in results))
    print("─" * w)

    for name, fn in [
        ("mean (s)",   lambda r: r.mean),
        ("median (s)", lambda r: r.median),
        ("stdev (s)",  lambda r: r.stdev),
        ("p95 (s)",    lambda r: r.p95),
        ("min (s)",    lambda r: r.minimum),
        ("max (s)",    lambda r: r.maximum),
    ]:
        print(f"{name:<12}" + "".join(f"{fn(r):>27.4f}" for r in results))

    print("─" * w)
    overhead = results[1].mean - results[0].mean
    pct      = (overhead / results[0].mean * 100) if results[0].mean > 0 else 0
    sign     = "+" if overhead >= 0 else ""
    print(f"  Overhead time travel (mean): {sign}{overhead:.4f}s  ({sign}{pct:.1f}%)")
    print("═" * w + "\n")

def plot_results(results: list[BenchmarkResult],
                 path: str = "benchmark_plot.png") -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("ℹ️  matplotlib no disponible; saltando gráfico.")
        return

    fig, (ax1) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Iceberg Time Travel Benchmark", fontsize=14, fontweight="bold")
    colors = ["#2563eb", "#dc2626"]

    runs = range(1, len(results[0].times) + 1)
    for r, c in zip(results, colors):
        ax1.plot(runs, r.times, marker="o", label=r.label, color=c, linewidth=2)
    ax1.set(xlabel="Run #", ylabel="Tiempo (s)", title="Tiempo por ejecución")
    ax1.legend(fontsize=8)
    ax1.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(path, dpi=150)
    print(f"📊 Gráfico guardado en: {path}")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Iceberg time travel benchmark con Spark")
    p.add_argument("--database", default=DEFAULT_DATABASE, help=f"Namespace / base de datos [default: {DEFAULT_DATABASE}]")
    p.add_argument("--table",    default=DEFAULT_TABLE,   help=f"Nombre de la tabla [default: {DEFAULT_TABLE}]")
    p.add_argument("--query",    default=DEFAULT_QUERY,
                   help="SQL a ejecutar. Usa {table} como placeholder "
                        "(default: 'SELECT COUNT(*) FROM {table}')")

    tt = p.add_mutually_exclusive_group()
    tt.add_argument("--snapshot-id",      type=int,
                    help="Snapshot ID exacto (VERSION AS OF)")
    tt.add_argument("--timestamp",
                    help="Timestamp ISO-8601 (TIMESTAMP AS OF), "
                         "ej: '2024-01-15 12:00:00'")
    tt.add_argument("--use-nth-snapshot", type=int, default=1, metavar="N",
                    help="Usa el N-ésimo snapshot más antiguo (1=penúltimo) [default: 1]")

    p.add_argument("--runs",           type=int, default=DEFAULT_RUNS)
    p.add_argument("--plot",           default="benchmark_plot.png")
    p.add_argument("--no-plot",        action="store_true")
    p.add_argument("--list-snapshots", action="store_true",
                   help="Solo listar snapshots disponibles y salir")
    args, _ = p.parse_known_args()
    return args


def main() -> None:
    args       = parse_args()
    full_table = f"{CATALOG_NAME}.{args.database}.{args.table}"
    sql        = args.query.replace("{table}", full_table)

    print(f"\n🧊 Iceberg Time Travel Benchmark")
    print(f"   Catálogo : {CATALOG_NAME}")
    print(f"   Tabla    : {full_table}")
    print(f"   Query    : {sql}")
    print(f"   Runs     : {args.runs}  (+ {WARMUP_RUNS} warmup)\n")

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Listar snapshots disponibles
    snapshots = get_snapshots(spark, full_table)
    print(f"📋 Snapshots disponibles ({len(snapshots)}):")
    for i, s in enumerate(snapshots):
        tag = " ← actual" if i == 0 else ""
        print(f"   [{i}] id={s['snapshot_id']}  "
              f"at={s['committed_at']}  op={s['operation']}{tag}")

    if args.list_snapshots:
        spark.stop()
        return

    if len(snapshots) < 2:
        print("\n⚠️  La tabla solo tiene un snapshot; necesitas al menos 2 para comparar.")
        spark.stop()
        return

    # Resolver qué snapshot/timestamp usar
    snapshot_id: Optional[int] = args.snapshot_id
    timestamp:   Optional[str] = args.timestamp

    if snapshot_id is None and timestamp is None:
        n           = min(args.use_nth_snapshot, len(snapshots) - 1)
        snapshot_id = snapshots[n]["snapshot_id"]
        print(f"\n🔍 Usando snapshot [{n}]  "
              f"id={snapshot_id}  at={snapshots[n]['committed_at']}")

    sql_tt = build_timetravel_sql(sql, snapshot_id, timestamp)
    print(f"\n   SQL actual     : {sql}")
    print(f"   SQL timetravel : {sql_tt}\n")

    # Ejecutar y reportar
    current_result, tt_result = run_benchmark(spark, sql, sql_tt, args.runs)
    print_report([current_result, tt_result], sql)
    if not args.no_plot:
        plot_results([current_result, tt_result], args.plot)

    spark.stop()


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
import os
import signal
import sys
import time
from dataclasses import dataclass
from threading import Event
from typing import List, Dict, Any, Tuple

# === Config ===
STOP_EVENT = Event()
SLEEP_SECS = float(os.getenv("CLEANER_SLEEP_SECS", "2"))                 # sleep por paso
POST_CYCLE_SLEEP_SECS = float(os.getenv("POST_CYCLE_SLEEP_SECS", "3"))   # sleep post-ciclo

def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

# --------------------------------------------------------------------
# Simulación de dos colas (dos bloques de datos distintos) del middleware
# --------------------------------------------------------------------

# Cola A: resultados de "conteos" (ejemplo: totales por período o globales)
# Estructura esperada: {"label": <str>, "cantidad": <int>}
Q2_COUNTS_DATA: List[Dict[str, Any]] = [
    {"label": "2024-Q1", "cantidad": 1240},
    {"label": "2024-Q2", "cantidad": 1398},
    {"label": "2025-Q1", "cantidad": 1512},
    {"label": "TOTAL",   "cantidad": 4150},
]

# Cola B: productos que más ganancias generaron (nombre y monto)
# Estructura esperada: {"item_name": <str>, "monto": <float>}
Q2_TOP_PRODUCTS_DATA: List[Dict[str, Any]] = [
    {"item_name": "Latte",         "monto":  9850.0},
    {"item_name": "Cappuccino",    "monto":  9120.5},
    {"item_name": "Flat White",    "monto":  8742.9},
    {"item_name": "Matcha Latte",  "monto":  8211.0},
    {"item_name": "Mocha",         "monto":  7999.0},
]

# --------------------------------------------------------------------
# Structs de salida
# --------------------------------------------------------------------
@dataclass
class CountResult:
    label: str
    cantidad: int

@dataclass
class TopProduct:
    item_name: str
    monto: float

def handle_sigterm(signum, frame):
    print("\n[q2_output_builder] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[q2_output_builder] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# --------------------------------------------------------------------
# 1) Procesar información (construye los structs de ambas colas)
# --------------------------------------------------------------------
def procesar_informacion() -> Tuple[List[CountResult], List[TopProduct]]:
    """
    Simula la lectura de dos colas distintas del middleware (conteos y top productos)
    y crea dos colecciones de structs listos para enviar.
    """
    print("[q2_output_builder] OBTENIENDO INFORMACIÓN… (fuente: memoria, 2 colas)", flush=True)

    # Cola A (conteos)
    counts_rows = [dict(row) for row in Q2_COUNTS_DATA]
    print(f"[q2_output_builder] Cola A (conteos): {len(counts_rows)} registros.", flush=True)

    # Cola B (top productos)
    top_rows = [dict(row) for row in Q2_TOP_PRODUCTS_DATA]
    print(f"[q2_output_builder] Cola B (top productos): {len(top_rows)} registros.", flush=True)

    counts: List[CountResult] = []
    for r in counts_rows:
        try:
            label = str(r.get("label", "")).strip()
            cantidad = int(r.get("cantidad", 0))
            if not label:
                continue
            counts.append(CountResult(label=label, cantidad=cantidad))
        except (TypeError, ValueError):
            continue

    top_products: List[TopProduct] = []
    for r in top_rows:
        try:
            name = str(r.get("item_name", "")).strip()
            monto = float(r.get("monto", 0))
            if not name:
                continue
            top_products.append(TopProduct(item_name=name, monto=monto))
        except (TypeError, ValueError):
            continue

    print(f"[q2_output_builder] PROCESANDO INFORMACIÓN… (conteos={len(counts)}, top={len(top_products)})", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

    return counts, top_products

# --------------------------------------------------------------------
# 2) Enviar información (por ahora, imprimir ambos resultados)
# --------------------------------------------------------------------
def enviar_informacion(counts: List[CountResult], top_products: List[TopProduct]) -> None:
    """
    En el futuro publicará en dos colas del middleware. Por ahora imprime todo.
    """
    print("[q2_output_builder] MANDANDO INFORMACIÓN…", flush=True)

    print("\n[q2_output_builder] === Conteos (Subquery A) ===", flush=True)
    for c in counts:
        print(f"[q2_output_builder] -> label={c.label}, cantidad={c.cantidad}", flush=True)

    print("\n[q2_output_builder] === Top productos por ganancias (Subquery B) ===", flush=True)
    for tp in top_products:
        print(f"[q2_output_builder] -> item_name={tp.item_name}, monto={tp.monto}", flush=True)

    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers de señales
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[q2_output_builder] Iniciado. PID:", os.getpid(), flush=True)

    while not STOP_EVENT.is_set():
        counts, top_products = procesar_informacion()
        if STOP_EVENT.is_set():
            break
        enviar_informacion(counts, top_products)

        # Sleep y limpiar pantalla al final del ciclo (demo loop)
        for _ in range(int(POST_CYCLE_SLEEP_SECS)):
            if STOP_EVENT.is_set():
                break
            time.sleep(1.0)
        if STOP_EVENT.is_set():
            break
        _clear_screen()

    print("[q2_output_builder] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[q2_output_builder] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

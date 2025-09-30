#!/usr/bin/env python3
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from threading import Event
from typing import List, Dict, Any, Tuple
from collections import defaultdict

# === Config ===
STOP_EVENT = Event()
SLEEP_SECS = float(os.getenv("CLEANER_SLEEP_SECS", "2"))                 # sleep por paso
POST_CYCLE_SLEEP_SECS = float(os.getenv("POST_CYCLE_SLEEP_SECS", "3"))   # sleep post-ciclo

def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

# ------------------------------------------------------------
# Dataset en memoria (simula lo que manda el middleware)
# Campos: store_id, created_at, final_amount
# (Ya vienen filtradas a 06:00–23:00 y a 2024–2025)
# ------------------------------------------------------------
TX_DATA: List[Dict[str, Any]] = [
    # 2024 - S1
    {"store_id": 5, "created_at": "2024-01-01 10:15:00", "final_amount":  79.0},
    {"store_id": 5, "created_at": "2024-03-15 18:45:10", "final_amount": 150.5},
    {"store_id": 8, "created_at": "2024-06-30 22:59:59", "final_amount":  95.0},
    # 2024 - S2
    {"store_id": 5, "created_at": "2024-07-09 06:30:05", "final_amount":  75.0},
    {"store_id": 5, "created_at": "2024-09-01 22:59:59", "final_amount":  99.9},
    {"store_id": 8, "created_at": "2024-12-15 12:00:00", "final_amount": 130.0},
    # 2025 - S1
    {"store_id": 5, "created_at": "2025-01-10 11:00:00", "final_amount": 120.0},
    {"store_id": 8, "created_at": "2025-03-21 19:40:00", "final_amount":  88.8},
    {"store_id": 8, "created_at": "2025-06-01 07:05:00", "final_amount": 200.0},
    # 2025 - S2
    {"store_id": 5, "created_at": "2025-07-01 10:00:00", "final_amount":  60.0},
    {"store_id": 5, "created_at": "2025-11-11 20:20:20", "final_amount": 145.3},
    {"store_id": 8, "created_at": "2025-12-31 22:59:59", "final_amount": 175.0},
]

# ------------------------------------------------------------
# Struct de salida: TPV por (year, semester, store_id)
# ------------------------------------------------------------
@dataclass
class SemesterTPV:
    year: int
    semester: int   # 1 (Ene-Jun) o 2 (Jul-Dic)
    store_id: int
    tpv: float

def handle_sigterm(signum, frame):
    print("\n[sum_transactions] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[sum_transactions] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# ------------------------------------------------------------
# 1) Procesar información: reduce por (año, semestre, store_id)
# ------------------------------------------------------------
def _semester_from_month(m: int) -> int:
    return 1 if 1 <= m <= 6 else 2

def procesar_informacion() -> List[SemesterTPV]:
    print("[sum_transactions] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)

    # acumulador: clave=(year, semester, store_id) -> suma de final_amount
    acc: Dict[Tuple[int, int, int], float] = defaultdict(float)

    for tx in TX_DATA:
        try:
            dt = datetime.strptime(str(tx["created_at"]).strip(), "%Y-%m-%d %H:%M:%S")
            year = dt.year
            month = dt.month
            sem = _semester_from_month(month)
            store_id = int(tx["store_id"])
            final_amount = float(tx["final_amount"])
        except Exception:
            continue
        acc[(year, sem, store_id)] += final_amount

    items: List[SemesterTPV] = [
        SemesterTPV(year=k[0], semester=k[1], store_id=k[2], tpv=round(v, 2))
        for k, v in acc.items()
    ]

    print(f"[sum_transactions] PROCESANDO INFORMACIÓN… ({len(items)} filas agregadas)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# ------------------------------------------------------------
# 2) Enviar información: imprimir resultados
# ------------------------------------------------------------
def enviar_informacion(items: List[SemesterTPV]) -> None:
    print("[sum_transactions] MANDANDO INFORMACIÓN…", flush=True)

    # Orden para legibilidad: year, semester, store_id
    items_sorted = sorted(items, key=lambda x: (x.year, x.semester, x.store_id))
    current = None
    for it in items_sorted:
        header = (it.year, it.semester)
        if header != current:
            y, s = header
            print(f"\n[sum_transactions] === {y} - S{s} ===", flush=True)
            current = header
        print(f"[sum_transactions] -> store_id={it.store_id}, TPV={it.tpv}", flush=True)

    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[sum_transactions] Iniciado. PID:", os.getpid(), flush=True)

    while not STOP_EVENT.is_set():
        items = procesar_informacion()
        if STOP_EVENT.is_set():
            break
        enviar_informacion(items)

        # Sleep y limpiar pantalla al final del ciclo
        for _ in range(int(POST_CYCLE_SLEEP_SECS)):
            if STOP_EVENT.is_set():
                break
            time.sleep(1.0)
        if STOP_EVENT.is_set():
            break
        _clear_screen()

    print("[sum_transactions] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[sum_transactions] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

#!/usr/bin/env python3
import os
import signal
import sys
import time
from dataclasses import dataclass
from threading import Event
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from datetime import datetime

# === Config ===
STOP_EVENT = Event()
SLEEP_SECS = float(os.getenv("CLEANER_SLEEP_SECS", "2"))                 # sleep por paso
POST_CYCLE_SLEEP_SECS = float(os.getenv("POST_CYCLE_SLEEP_SECS", "3"))   # sleep post-ciclo

def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

# --------------------------------------------------------------------
# Dataset en memoria (extractos de transacciones)
# Campos: user_id, customer_id, store_id, created_at
# --------------------------------------------------------------------
TX_DATA: List[Dict[str, Any]] = [
    {"transaction_id": "t-001", "store_id": 5, "user_id": 17585, "customer_id": 90001, "created_at": "2024-01-06 10:06:50"},
    {"transaction_id": "t-002", "store_id": 5, "user_id": 17585, "customer_id": 90001, "created_at": "2024-01-12 12:15:00"},
    {"transaction_id": "t-003", "store_id": 8, "user_id": 17586, "customer_id": 90002, "created_at": "2024-01-20 18:30:10"},
    {"transaction_id": "t-004", "store_id": 5, "user_id": 17590, "customer_id": 90003, "created_at": "2024-01-25 09:05:35"},
    {"transaction_id": "t-005", "store_id": 8, "user_id": 17586, "customer_id": 90002, "created_at": "2025-02-14 11:20:20"},
    {"transaction_id": "t-006", "store_id": 8, "user_id": 17586, "customer_id": 90002, "created_at": "2025-03-10 19:55:00"},
    {"transaction_id": "t-007", "store_id": 5, "user_id": 17585, "customer_id": 90001, "created_at": "2025-04-05 10:00:00"},
    {"transaction_id": "t-008", "store_id": 5, "user_id": 17590, "customer_id": 90003, "created_at": "2025-05-15 19:40:00"},
]

# --------------------------------------------------------------------
# Struct de salida: conteo por (year, store_id, user_id)
# --------------------------------------------------------------------
@dataclass
class YearlyUserPurchases:
    year: int
    store_id: int
    user_id: int
    purchases_count: int

def handle_sigterm(signum, frame):
    print("\n[count_purchases] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[count_purchases] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# ------------------------------------------------------------
# 1) Procesar información: reduce por (año, store_id, user_id)
# ------------------------------------------------------------
def procesar_informacion() -> List[YearlyUserPurchases]:
    print("[count_purchases] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)

    # conteo por clave
    counter: Dict[Tuple[int, int, int], int] = defaultdict(int)

    for tx in TX_DATA:
        try:
            created_at = datetime.strptime(str(tx["created_at"]).strip(), "%Y-%m-%d %H:%M:%S")
            year = created_at.year
            store_id = int(tx["store_id"])
            user_id = int(tx["user_id"])
        except Exception:
            continue
        counter[(year, store_id, user_id)] += 1

    items: List[YearlyUserPurchases] = [
        YearlyUserPurchases(year=key[0], store_id=key[1], user_id=key[2], purchases_count=count)
        for key, count in counter.items()
    ]

    print(f"[count_purchases] PROCESANDO INFORMACIÓN… ({len(items)} filas agregadas)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# ------------------------------------------------------------
# 2) Enviar información: imprimir resultados
# ------------------------------------------------------------
def enviar_informacion(items: List[YearlyUserPurchases]) -> None:
    print("[count_purchases] MANDANDO INFORMACIÓN…", flush=True)

    # Orden: year, store_id, purchases desc, user_id
    items_sorted = sorted(items, key=lambda x: (x.year, x.store_id, -x.purchases_count, x.user_id))
    current_header = None
    for it in items_sorted:
        header = (it.year, it.store_id)
        if header != current_header:
            y, sid = header
            print(f"\n[count_purchases] === {y} | store_id={sid} ===", flush=True)
            current_header = header
        print(f"[count_purchases] -> user_id={it.user_id}, purchases={it.purchases_count}", flush=True)

    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers de señales
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[count_purchases] Iniciado. PID:", os.getpid(), flush=True)

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

    print("[count_purchases] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[count_purchases] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

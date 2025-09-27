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
SLEEP_SECS = float(os.getenv("CLEANER_SLEEP_SECS", "2"))
POST_CYCLE_SLEEP_SECS = float(os.getenv("POST_CYCLE_SLEEP_SECS", "3"))

def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

# --- MISMO dataset que usa sum_transactions_items ---
TX_ITEMS_DATA: List[Dict[str, Any]] = [
    # 2024-01
    {"item_id": 1, "created_at": "2024-01-01 10:05:00", "quantity": 1, "subtotal": 6.0},
    {"item_id": 4, "created_at": "2024-01-10 12:00:00", "quantity": 2, "subtotal": 16.0},
    {"item_id": 8, "created_at": "2024-01-15 18:40:00", "quantity": 3, "subtotal": 30.0},
    # 2024-02
    {"item_id": 2, "created_at": "2024-02-02 09:00:00", "quantity": 2, "subtotal": 14.0},
    {"item_id": 6, "created_at": "2024-02-14 20:15:00", "quantity": 1, "subtotal": 9.5},
    {"item_id": 8, "created_at": "2024-02-20 11:30:00", "quantity": 2, "subtotal": 20.0},
    # 2024-03
    {"item_id": 4, "created_at": "2024-03-03 08:22:45", "quantity": 3, "subtotal": 24.0},
    {"item_id": 6, "created_at": "2024-03-10 19:55:00", "quantity": 1, "subtotal": 9.5},
    {"item_id": 8, "created_at": "2024-03-28 20:10:00", "quantity": 1, "subtotal": 10.0},
    # 2025-01
    {"item_id": 1, "created_at": "2025-01-05 10:00:00", "quantity": 1, "subtotal": 6.0},
    {"item_id": 2, "created_at": "2025-01-08 11:30:00", "quantity": 3, "subtotal": 21.0},
    {"item_id": 6, "created_at": "2025-01-12 12:45:00", "quantity": 2, "subtotal": 19.0},
    # 2025-02
    {"item_id": 4, "created_at": "2025-02-02 10:20:00", "quantity": 2, "subtotal": 16.0},
    {"item_id": 4, "created_at": "2025-02-15 19:40:00", "quantity": 1, "subtotal": 8.0},
    {"item_id": 8, "created_at": "2025-02-28 21:10:00", "quantity": 3, "subtotal": 30.0},
]

# Struct de salida (cantidad mensual por item)
@dataclass
class ItemMonthlyCount:
    year: int
    month: int
    item_id: int
    quantity: int  # suma de 'quantity' por (año, mes, item_id)

def handle_sigterm(signum, frame):
    print("\n[count_transaction_items] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[count_transaction_items] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# 1) Reduce: suma 'quantity' por (año, mes, item_id)
def procesar_informacion() -> List[ItemMonthlyCount]:
    print("[count_transaction_items] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)

    acc: Dict[Tuple[int, int, int], int] = defaultdict(int)

    for row in TX_ITEMS_DATA:
        try:
            dt = datetime.strptime(str(row["created_at"]).strip(), "%Y-%m-%d %H:%M:%S")
            y, m = dt.year, dt.month
            item_id = int(row["item_id"])
            qty = int(row["quantity"])
        except Exception:
            continue
        acc[(y, m, item_id)] += qty

    items = [ItemMonthlyCount(year=y, month=m, item_id=i, quantity=qty)
             for (y, m, i), qty in acc.items()]

    print(f"[count_transaction_items] PROCESANDO INFORMACIÓN… ({len(items)} filas agregadas)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# 2) Enviar: imprimir **TODOS** los items por mes (ordenados por cantidad desc)
def enviar_informacion(items: List[ItemMonthlyCount]) -> None:
    print("[count_transaction_items] MANDANDO INFORMACIÓN…", flush=True)

    by_month: Dict[Tuple[int, int], List[ItemMonthlyCount]] = defaultdict(list)
    for it in items:
        by_month[(it.year, it.month)].append(it)

    for key in sorted(by_month.keys()):
        y, m = key
        print(f"\n[count_transaction_items] === {y}-{m:02d} ===", flush=True)
        for rec in sorted(by_month[key], key=lambda x: (-x.quantity, x.item_id)):
            print(f"[count_transaction_items] item_id={rec.item_id}, quantity={rec.quantity}", flush=True)

    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[count_transaction_items] Iniciado. PID:", os.getpid(), flush=True)

    while not STOP_EVENT.is_set():
        items = procesar_informacion()
        if STOP_EVENT.is_set(): break
        enviar_informacion(items)

        for _ in range(int(POST_CYCLE_SLEEP_SECS)):
            if STOP_EVENT.is_set(): break
            time.sleep(1.0)
        if STOP_EVENT.is_set(): break
        _clear_screen()

    print("[count_transaction_items] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[count_transaction_items] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

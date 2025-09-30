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

# --- Dataset en memoria (MISMO para ambos reduces) ---
# Campos: item_id, created_at, quantity, subtotal
# NOTA: 'subtotal' YA es unit_price * quantity (¡no lo multipliques de nuevo!)
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

# Struct de salida (monto mensual por item)
@dataclass
class ItemMonthlyRevenue:
    year: int
    month: int
    item_id: int
    revenue: float  # suma de 'subtotal' por (año, mes, item_id)

def handle_sigterm(signum, frame):
    print("\n[sum_transactions_items] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[sum_transactions_items] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# 1) Reduce: suma 'subtotal' por (año, mes, item_id)
def procesar_informacion() -> List[ItemMonthlyRevenue]:
    print("[sum_transactions_items] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)
    acc: Dict[Tuple[int, int, int], float] = defaultdict(float)

    for row in TX_ITEMS_DATA:
        try:
            dt = datetime.strptime(str(row["created_at"]).strip(), "%Y-%m-%d %H:%M:%S")
            y, m = dt.year, dt.month
            item_id = int(row["item_id"])
            subtotal = float(row["subtotal"])  # ¡YA es unit*qty!
        except Exception:
            continue
        acc[(y, m, item_id)] += subtotal

    items = [ItemMonthlyRevenue(year=y, month=m, item_id=i, revenue=round(total, 2))
             for (y, m, i), total in acc.items()]

    print(f"[sum_transactions_items] PROCESANDO INFORMACIÓN… ({len(items)} filas agregadas)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# 2) Enviar: imprimir **TODOS** los items por mes (ordenados por revenue desc)
def enviar_informacion(items: List[ItemMonthlyRevenue]) -> None:
    print("[sum_transactions_items] MANDANDO INFORMACIÓN…", flush=True)

    by_month: Dict[Tuple[int, int], List[ItemMonthlyRevenue]] = defaultdict(list)
    for it in items:
        by_month[(it.year, it.month)].append(it)

    for key in sorted(by_month.keys()):
        y, m = key
        print(f"\n[sum_transactions_items] === {y}-{m:02d} ===", flush=True)
        for rec in sorted(by_month[key], key=lambda x: (-x.revenue, x.item_id)):
            print(f"[sum_transactions_items] item_id={rec.item_id}, revenue={rec.revenue}", flush=True)

    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[sum_transactions_items] Iniciado. PID:", os.getpid(), flush=True)

    while not STOP_EVENT.is_set():
        items = procesar_informacion()
        if STOP_EVENT.is_set(): break
        enviar_informacion(items)

        for _ in range(int(POST_CYCLE_SLEEP_SECS)):
            if STOP_EVENT.is_set(): break
            time.sleep(1.0)
        if STOP_EVENT.is_set(): break
        _clear_screen()

    print("[sum_transactions_items] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[sum_transactions_items] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

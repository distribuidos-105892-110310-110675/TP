#!/usr/bin/env python3
import os
import signal
import sys
import time
from dataclasses import dataclass
from threading import Event
from typing import List, Dict, Any

# === Config ===
STOP_EVENT = Event()
SLEEP_SECS = float(os.getenv("CLEANER_SLEEP_SECS", "2"))                 # sleep de pasos
POST_CYCLE_SLEEP_SECS = float(os.getenv("POST_CYCLE_SLEEP_SECS", "3"))   # sleep post-ciclo

def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

# --- Dataset en memoria (equivale a .data/transaction_items/...) ---
# Columns: transaction_id,item_id,quantity,unit_price,subtotal,created_at
TRANSACTION_ITEMS_DATA: List[Dict[str, Any]] = [
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2024-01-01 10:06:50"},
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:50"},
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "8", "quantity": "3", "unit_price": "10.0", "subtotal": "30.0", "created_at": "2024-01-01 10:06:50"},
    {"transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3", "item_id": "2", "quantity": "3", "unit_price": "7.0", "subtotal": "21.0", "created_at": "2024-01-01 10:06:52"},
    {"transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3", "item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2024-01-01 10:06:52"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "1", "quantity": "1", "unit_price": "6.0", "subtotal": "6.0", "created_at": "2024-01-01 10:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:53"},
]

# Struct requerido: item_id, created_at, quantity, subtotal
@dataclass
class TransactionItem:
    item_id: int
    created_at: str
    quantity: int
    subtotal: float

def handle_sigterm(signum, frame):
    print("\n[transaction_items_cleaner] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[transaction_items_cleaner] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# ------------------------------------------------------------
# 1) Obtener información (desde memoria)
# ------------------------------------------------------------
def obtener_informacion() -> List[Dict[str, Any]]:
    print("[transaction_items_cleaner] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)
    rows = [dict(row) for row in TRANSACTION_ITEMS_DATA]
    print(f"[transaction_items_cleaner] Leídas {len(rows)} filas desde memoria.", flush=True)
    return rows

# ------------------------------------------------------------
# 2) Procesar información (genera vector de structs)
# ------------------------------------------------------------
def procesar_informacion() -> List[TransactionItem]:
    """
    Construye TransactionItem con: item_id, created_at, quantity, subtotal.
    """
    if STOP_EVENT.is_set():
        return []

    rows = obtener_informacion()
    items: List[TransactionItem] = []
    for r in rows:
        try:
            item_id = int(str(r.get("item_id", "0")).strip())
            created_at = str(r.get("created_at", "")).strip()
            quantity = int(str(r.get("quantity", "0")).strip())
            subtotal = float(str(r.get("subtotal", "0")).strip())
            if not created_at:
                continue
            items.append(TransactionItem(item_id=item_id, created_at=created_at, quantity=quantity, subtotal=subtotal))
        except ValueError:
            continue

    print(f"[transaction_items_cleaner] LIMPIANDO / PROCESANDO INFORMACIÓN… ({len(items)} items)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# ------------------------------------------------------------
# 3) Enviar información (por ahora, imprimir los structs)
# ------------------------------------------------------------
def enviar_informacion(items: List[TransactionItem]) -> None:
    print("[transaction_items_cleaner] MANDANDO INFORMACIÓN…", flush=True)
    for it in items:
        print(
            f"[transaction_items_cleaner] -> item_id={it.item_id}, created_at={it.created_at}, "
            f"quantity={it.quantity}, subtotal={it.subtotal}",
            flush=True,
        )
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers de señales (NO TOCAR)
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[transaction_items_cleaner] Iniciado. PID:", os.getpid(), flush=True)

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

    print("[transaction_items_cleaner] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[transaction_items_cleaner] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

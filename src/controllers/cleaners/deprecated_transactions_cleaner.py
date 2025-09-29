#!/usr/bin/env python3
import os
import signal
import sys
import time
from dataclasses import dataclass
from threading import Event
from typing import List, Dict, Any, Optional

# === Config ===
STOP_EVENT = Event()
SLEEP_SECS = float(os.getenv("CLEANER_SLEEP_SECS", "2"))                 # sleep de pasos
POST_CYCLE_SLEEP_SECS = float(os.getenv("POST_CYCLE_SLEEP_SECS", "3"))   # sleep post-ciclo

def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

# --- Dataset en memoria (equivale a .data/transactions/...) ---
# Columns:
# transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
TRANSACTIONS_DATA: List[Dict[str, Any]] = [
    {
        "transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2",
        "store_id": "5",
        "payment_method_id": "1",
        "voucher_id": "",
        "user_id": "3",
        "original_amount": "63.5",
        "discount_applied": "0.0",
        "final_amount": "63.5",
        "created_at": "2024-01-01 10:06:50",
    },
    {
        "transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3",
        "store_id": "8",
        "payment_method_id": "2",
        "voucher_id": "",
        "user_id": "5",
        "original_amount": "30.5",
        "discount_applied": "0.0",
        "final_amount": "30.5",
        "created_at": "2024-01-01 10:06:52",
    },
    {
        "transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c",
        "store_id": "5",
        "payment_method_id": "4",
        "voucher_id": "",
        "user_id": "6",
        "original_amount": "54.0",
        "discount_applied": "0.0",
        "final_amount": "54.0",
        "created_at": "2024-01-01 10:06:53",
    },
]

# Struct requerido: transaction_id, created_at, final_amount, store_id, user_id
@dataclass
class TransactionInfo:
    transaction_id: str
    created_at: str         # lo dejamos como string legible (podemos parsear más adelante si hace falta)
    final_amount: float
    store_id: int
    user_id: Optional[str]  # puede venir vacío

def handle_sigterm(signum, frame):
    print("\n[transactions_cleaner] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[transactions_cleaner] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# ------------------------------------------------------------
# 1) Obtener información (desde memoria)
# ------------------------------------------------------------
def obtener_informacion() -> List[Dict[str, Any]]:
    print("[transactions_cleaner] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)
    rows = [dict(row) for row in TRANSACTIONS_DATA]
    print(f"[transactions_cleaner] Leídas {len(rows)} filas desde memoria.", flush=True)
    return rows

# ------------------------------------------------------------
# 2) Procesar información (genera vector de structs)
# ------------------------------------------------------------
def procesar_informacion() -> List[TransactionInfo]:
    """
    Llama a obtener_informacion() y construye TransactionInfo
    con: transaction_id, created_at, final_amount, store_id, user_id
    """
    if STOP_EVENT.is_set():
        return []

    rows = obtener_informacion()
    items: List[TransactionInfo] = []
    for r in rows:
        try:
            transaction_id = r.get("transaction_id", "").strip()
            created_at = r.get("created_at", "").strip()
            final_amount_raw = r.get("final_amount", "0").strip()
            store_id_raw = r.get("store_id", "0").strip()
            user_id_raw = r.get("user_id", "").strip()

            if not transaction_id or not created_at:
                continue

            final_amount = float(final_amount_raw)
            store_id = int(store_id_raw)
            user_id = user_id_raw if user_id_raw != "" else None

            items.append(
                TransactionInfo(
                    transaction_id=transaction_id,
                    created_at=created_at,
                    final_amount=final_amount,
                    store_id=store_id,
                    user_id=user_id,
                )
            )
        except ValueError:
            # Si no se pueden castear numeric fields, lo salteamos
            continue

    print(f"[transactions_cleaner] LIMPIANDO / PROCESANDO INFORMACIÓN… ({len(items)} items)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# ------------------------------------------------------------
# 3) Enviar información (por ahora, imprimir los structs)
# ------------------------------------------------------------
def enviar_informacion(items: List[TransactionInfo]) -> None:
    print("[transactions_cleaner] MANDANDO INFORMACIÓN…", flush=True)
    for it in items:
        print(
            "[transactions_cleaner] -> "
            f"transaction_id={it.transaction_id}, created_at={it.created_at}, "
            f"final_amount={it.final_amount}, store_id={it.store_id}, user_id={it.user_id}",
            flush=True,
        )
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers de señales (NO TOCAR)
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[transactions_cleaner] Iniciado. PID:", os.getpid(), flush=True)

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

    print("[transactions_cleaner] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[transactions_cleaner] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

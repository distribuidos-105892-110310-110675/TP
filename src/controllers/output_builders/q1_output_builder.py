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
SLEEP_SECS = float(os.getenv("CLEANER_SLEEP_SECS", "2"))                 # sleep por paso
POST_CYCLE_SLEEP_SECS = float(os.getenv("POST_CYCLE_SLEEP_SECS", "3"))   # sleep post-ciclo

def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

# --- Dataset en memoria (ya filtrado por el sistema distribuido) ---
# Objetivo: transacciones 2024–2025, 06:00–23:00, final_amount >= 75
# Campos relevantes: transaction_id, created_at, final_amount
Q1_RESULTS_DATA: List[Dict[str, Any]] = [
    {"transaction_id": "11111111-1111-1111-1111-111111111111", "created_at": "2024-01-01 10:15:00", "final_amount":  79.0},
    {"transaction_id": "22222222-2222-2222-2222-222222222222", "created_at": "2024-03-15 18:45:10", "final_amount": 150.5},
    {"transaction_id": "33333333-3333-3333-3333-333333333333", "created_at": "2025-07-09 06:30:05", "final_amount":  75.0},
    {"transaction_id": "44444444-4444-4444-4444-444444444444", "created_at": "2025-09-01 22:59:59", "final_amount":  99.9},
]

# Struct a emitir
@dataclass
class TransactionOut:
    transaction_id: str
    created_at: str
    final_amount: float

def handle_sigterm(signum, frame):
    print("\n[q1_output_builder] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[q1_output_builder] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# ------------------------------------------------------------
# 1) Procesar información (arma los structs a partir de memoria)
# ------------------------------------------------------------
def procesar_informacion() -> List[TransactionOut]:
    """
    Simula el 'pasamanos' de resultados: toma los registros ya filtrados
    (provenientes del middleware en el futuro) y crea los structs de salida.
    """
    print("[q1_output_builder] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)

    rows = [dict(row) for row in Q1_RESULTS_DATA]  # copia superficial defensiva
    print(f"[q1_output_builder] Leídos {len(rows)} resultados.", flush=True)

    items: List[TransactionOut] = []
    for r in rows:
        try:
            txid = str(r.get("transaction_id", "")).strip()
            created_at = str(r.get("created_at", "")).strip()
            final_amount = float(r.get("final_amount", 0))
            if not txid or not created_at:
                continue
            items.append(TransactionOut(transaction_id=txid, created_at=created_at, final_amount=final_amount))
        except (TypeError, ValueError):
            continue

    print(f"[q1_output_builder] PROCESANDO INFORMACIÓN… ({len(items)} items)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# ------------------------------------------------------------
# 2) Enviar información (por ahora, imprimir)
# ------------------------------------------------------------
def enviar_informacion(items: List[TransactionOut]) -> None:
    """
    En el futuro publicará en el middleware. Por ahora imprime los structs.
    """
    print("[q1_output_builder] MANDANDO INFORMACIÓN…", flush=True)
    for it in items:
        print(
            f"[q1_output_builder] -> transaction_id={it.transaction_id}, "
            f"created_at={it.created_at}, final_amount={it.final_amount}",
            flush=True,
        )
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers de señales
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[q1_output_builder] Iniciado. PID:", os.getpid(), flush=True)

    while not STOP_EVENT.is_set():
        items = procesar_informacion()
        if STOP_EVENT.is_set():
            break
        enviar_informacion(items)

        # Sleep y limpiar pantalla al final del ciclo (demo loop)
        for _ in range(int(POST_CYCLE_SLEEP_SECS)):
            if STOP_EVENT.is_set():
                break
            time.sleep(1.0)
        if STOP_EVENT.is_set():
            break
        _clear_screen()

    print("[q1_output_builder] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[q1_output_builder] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

#!/usr/bin/env python3
import os
import signal
import sys
import time
from threading import Event

STOP_EVENT = Event()
SLEEP_SECS = float(os.getenv("CLEANER_SLEEP_SECS", "2"))

def handle_sigterm(signum, frame):
    print("\n[sum_transactions] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[sum_transactions] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

def obtener_informacion():
    print("[sum_transactions] OBTENIENDO INFORMACIÓN…", flush=True)
    time.sleep(SLEEP_SECS)

def limpiar_informacion():
    print("[sum_transactions] LIMPIANDO INFORMACIÓN…", flush=True)
    time.sleep(SLEEP_SECS)

def mandar_informacion():
    print("[sum_transactions] MANDANDO INFORMACIÓN…", flush=True)
    time.sleep(SLEEP_SECS)

def main():
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[sum_transactions] Iniciado. PID:", os.getpid(), flush=True)

    while not STOP_EVENT.is_set():
        obtener_informacion()
        if STOP_EVENT.is_set(): break
        limpiar_informacion()
        if STOP_EVENT.is_set(): break
        mandar_informacion()

    print("[sum_transactions] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[sum_transactions] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

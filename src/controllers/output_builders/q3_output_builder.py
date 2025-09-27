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

# --------------------------------------------------------------------
# Dataset en memoria (simula lo que enviará el middleware)
# Objetivo: TPV por semestre (S1=Ene-Jun, S2=Jul-Dic) de 2024 y 2025,
# por sucursal (usando store_name), considerando SOLO 06:00–23:00.
# Campos: store_name, year, semester, tpv
# --------------------------------------------------------------------
Q3_TPV_SEMESTRAL_DATA: List[Dict[str, Any]] = [
    # 2024 - S1
    {"store_name": "G Coffee @ USJ 89q",           "year": 2024, "semester": 1, "tpv":  48050.0},
    {"store_name": "G Coffee @ Kampung Changkat",  "year": 2024, "semester": 1, "tpv":  42110.5},
    {"store_name": "G Coffee @ Taman Damansara",   "year": 2024, "semester": 1, "tpv":  39999.9},
    # 2024 - S2
    {"store_name": "G Coffee @ USJ 89q",           "year": 2024, "semester": 2, "tpv":  50500.0},
    {"store_name": "G Coffee @ Kampung Changkat",  "year": 2024, "semester": 2, "tpv":  44775.4},
    {"store_name": "G Coffee @ Taman Damansara",   "year": 2024, "semester": 2, "tpv":  41230.7},
    # 2025 - S1
    {"store_name": "G Coffee @ USJ 89q",           "year": 2025, "semester": 1, "tpv":  53310.2},
    {"store_name": "G Coffee @ Kampung Changkat",  "year": 2025, "semester": 1, "tpv":  46888.0},
    {"store_name": "G Coffee @ Taman Damansara",   "year": 2025, "semester": 1, "tpv":  43990.0},
    # 2025 - S2
    {"store_name": "G Coffee @ USJ 89q",           "year": 2025, "semester": 2, "tpv":  55120.0},
    {"store_name": "G Coffee @ Kampung Changkat",  "year": 2025, "semester": 2, "tpv":  48200.0},
    {"store_name": "G Coffee @ Taman Damansara",   "year": 2025, "semester": 2, "tpv":  45555.5},
]

# Struct de salida
@dataclass
class SemesterTPV:
    store_name: str
    year: int
    semester: int  # 1 o 2
    tpv: float

def handle_sigterm(signum, frame):
    print("\n[q3_output_builder] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[q3_output_builder] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# ------------------------------------------------------------
# 1) Procesar información (arma los structs a partir del dataset)
# ------------------------------------------------------------
def procesar_informacion() -> List[SemesterTPV]:
    print("[q3_output_builder] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)

    rows = [dict(row) for row in Q3_TPV_SEMESTRAL_DATA]  # copia superficial defensiva
    print(f"[q3_output_builder] Registros recibidos: {len(rows)}", flush=True)

    items: List[SemesterTPV] = []
    for r in rows:
        try:
            store_name = str(r.get("store_name", "")).strip()
            year = int(r.get("year", 0))
            semester = int(r.get("semester", 0))
            tpv = float(r.get("tpv", 0))
            if not store_name or year not in (2024, 2025) or semester not in (1, 2):
                continue
            items.append(SemesterTPV(store_name=store_name, year=year, semester=semester, tpv=tpv))
        except (TypeError, ValueError):
            continue

    print(f"[q3_output_builder] PROCESANDO INFORMACIÓN… ({len(items)} items)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# ------------------------------------------------------------
# 2) Enviar información (por ahora, imprimir)
# ------------------------------------------------------------
def enviar_informacion(items: List[SemesterTPV]) -> None:
    print("[q3_output_builder] MANDANDO INFORMACIÓN…", flush=True)

    # Ordenadito por año, semestre, store_name para legibilidad
    items_sorted = sorted(items, key=lambda x: (x.year, x.semester, x.store_name.lower()))
    current_key = None
    for it in items_sorted:
        key = (it.year, it.semester)
        if key != current_key:
            year, sem = key
            print(f"\n[q3_output_builder] === Reporte {year} - S{sem} ===", flush=True)
            current_key = key
        print(f"[q3_output_builder] -> store_name={it.store_name}, TPV={it.tpv}", flush=True)

    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers de señales
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[q3_output_builder] Iniciado. PID:", os.getpid(), flush=True)

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

    print("[q3_output_builder] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[q3_output_builder] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

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
# Dataset en memoria (simula lo que enviará el middleware)
# Objetivo:
#   Para cada sucursal y semestre (S1=Ene-Jun, S2=Jul-Dic) de 2024–2025,
#   obtener los top 3 clientes por cantidad de compras y su fecha de cumpleaños.
# Campos mínimos que recibimos: store_name, year, semester, user_id, birthdate, purchases_count
# --------------------------------------------------------------------
Q4_TOP3_BIRTHDAYS_DATA: List[Dict[str, Any]] = [
    # 2024 - S1
    {"store_name": "G Coffee @ USJ 89q",          "year": 2024, "semester": 1, "user_id": 17585, "birthdate": "2003-01-06", "purchases_count": 22},
    {"store_name": "G Coffee @ USJ 89q",          "year": 2024, "semester": 1, "user_id": 17590, "birthdate": "1986-07-20", "purchases_count": 19},
    {"store_name": "G Coffee @ USJ 89q",          "year": 2024, "semester": 1, "user_id": 17593, "birthdate": "1986-02-20", "purchases_count": 18},

    {"store_name": "G Coffee @ Kampung Changkat", "year": 2024, "semester": 1, "user_id": 17591, "birthdate": "1997-05-20", "purchases_count": 21},
    {"store_name": "G Coffee @ Kampung Changkat", "year": 2024, "semester": 1, "user_id": 17586, "birthdate": "2002-03-29", "purchases_count": 20},
    {"store_name": "G Coffee @ Kampung Changkat", "year": 2024, "semester": 1, "user_id": 17588, "birthdate": "1979-09-14", "purchases_count": 17},

    # 2024 - S2
    {"store_name": "G Coffee @ USJ 89q",          "year": 2024, "semester": 2, "user_id": 17587, "birthdate": "1979-05-28", "purchases_count": 25},
    {"store_name": "G Coffee @ USJ 89q",          "year": 2024, "semester": 2, "user_id": 17592, "birthdate": "2008-07-05", "purchases_count": 21},
    {"store_name": "G Coffee @ USJ 89q",          "year": 2024, "semester": 2, "user_id": 17594, "birthdate": "2005-06-23", "purchases_count": 20},

    {"store_name": "G Coffee @ Kampung Changkat", "year": 2024, "semester": 2, "user_id": 17585, "birthdate": "2003-01-06", "purchases_count": 23},
    {"store_name": "G Coffee @ Kampung Changkat", "year": 2024, "semester": 2, "user_id": 17593, "birthdate": "1986-02-20", "purchases_count": 22},
    {"store_name": "G Coffee @ Kampung Changkat", "year": 2024, "semester": 2, "user_id": 17590, "birthdate": "1986-07-20", "purchases_count": 19},

    # 2025 - S1
    {"store_name": "G Coffee @ USJ 89q",          "year": 2025, "semester": 1, "user_id": 17586, "birthdate": "2002-03-29", "purchases_count": 26},
    {"store_name": "G Coffee @ USJ 89q",          "year": 2025, "semester": 1, "user_id": 17588, "birthdate": "1979-09-14", "purchases_count": 24},
    {"store_name": "G Coffee @ USJ 89q",          "year": 2025, "semester": 1, "user_id": 17591, "birthdate": "1997-05-20", "purchases_count": 22},

    {"store_name": "G Coffee @ Kampung Changkat", "year": 2025, "semester": 1, "user_id": 17587, "birthdate": "1979-05-28", "purchases_count": 25},
    {"store_name": "G Coffee @ Kampung Changkat", "year": 2025, "semester": 1, "user_id": 17592, "birthdate": "2008-07-05", "purchases_count": 21},
    {"store_name": "G Coffee @ Kampung Changkat", "year": 2025, "semester": 1, "user_id": 17594, "birthdate": "2005-06-23", "purchases_count": 20},

    # 2025 - S2
    {"store_name": "G Coffee @ USJ 89q",          "year": 2025, "semester": 2, "user_id": 17585, "birthdate": "2003-01-06", "purchases_count": 27},
    {"store_name": "G Coffee @ USJ 89q",          "year": 2025, "semester": 2, "user_id": 17590, "birthdate": "1986-07-20", "purchases_count": 24},
    {"store_name": "G Coffee @ USJ 89q",          "year": 2025, "semester": 2, "user_id": 17593, "birthdate": "1986-02-20", "purchases_count": 23},

    {"store_name": "G Coffee @ Kampung Changkat", "year": 2025, "semester": 2, "user_id": 17586, "birthdate": "2002-03-29", "purchases_count": 26},
    {"store_name": "G Coffee @ Kampung Changkat", "year": 2025, "semester": 2, "user_id": 17588, "birthdate": "1979-09-14", "purchases_count": 24},
    {"store_name": "G Coffee @ Kampung Changkat", "year": 2025, "semester": 2, "user_id": 17591, "birthdate": "1997-05-20", "purchases_count": 22},
]

# Struct de salida
@dataclass
class TopCustomerBirthday:
    store_name: str
    year: int
    semester: int   # 1 o 2
    user_id: int
    birthdate: str
    purchases_count: int

def handle_sigterm(signum, frame):
    print("\n[q4_output_builder] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[q4_output_builder] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# ------------------------------------------------------------
# 1) Procesar información (arma los structs)
# ------------------------------------------------------------
def procesar_informacion() -> List[TopCustomerBirthday]:
    print("[q4_output_builder] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)

    rows = [dict(row) for row in Q4_TOP3_BIRTHDAYS_DATA]
    print(f"[q4_output_builder] Registros recibidos: {len(rows)}", flush=True)

    items: List[TopCustomerBirthday] = []
    for r in rows:
        try:
            store_name = str(r.get("store_name", "")).strip()
            year = int(r.get("year", 0))
            semester = int(r.get("semester", 0))
            user_id = int(r.get("user_id", 0))
            birthdate = str(r.get("birthdate", "")).strip()
            purchases_count = int(r.get("purchases_count", 0))

            if not store_name or not birthdate or year not in (2024, 2025) or semester not in (1, 2):
                continue

            items.append(
                TopCustomerBirthday(
                    store_name=store_name,
                    year=year,
                    semester=semester,
                    user_id=user_id,
                    birthdate=birthdate,
                    purchases_count=purchases_count,
                )
            )
        except (TypeError, ValueError):
            continue

    print(f"[q4_output_builder] PROCESANDO INFORMACIÓN… ({len(items)} items)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# ------------------------------------------------------------
# 2) Enviar información (por ahora, imprimir)
# ------------------------------------------------------------
def enviar_informacion(items: List[TopCustomerBirthday]) -> None:
    print("[q4_output_builder] MANDANDO INFORMACIÓN…", flush=True)

    # Orden para legibilidad: store_name, year, semester, purchases_count desc
    items_sorted = sorted(items, key=lambda x: (x.store_name.lower(), x.year, x.semester, -x.purchases_count))

    current_group: Tuple[str, int, int] = ("", 0, 0)
    rank = 0
    for it in items_sorted:
        group = (it.store_name, it.year, it.semester)
        if group != current_group:
            store, year, sem = group
            print(f"\n[q4_output_builder] === {store} | {year} - S{sem} (Top 3) ===", flush=True)
            current_group = group
            rank = 0
        rank += 1
        print(
            f"[q4_output_builder] {rank}) user_id={it.user_id}, birthdate={it.birthdate}, "
            f"purchases={it.purchases_count}",
            flush=True,
        )

    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers de señales
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[q4_output_builder] Iniciado. PID:", os.getpid(), flush=True)

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

    print("[q4_output_builder] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[q4_output_builder] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

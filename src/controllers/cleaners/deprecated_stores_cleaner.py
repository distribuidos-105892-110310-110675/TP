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

# --- Dataset en memoria (equivale a .data/stores/stores.csv) ---
# Columns: store_id,store_name,street,postal_code,city,state,latitude,longitude
STORES_DATA: List[Dict[str, Any]] = [
    {"store_id": "1",  "store_name": "G Coffee @ USJ 89q",            "street": "Jalan Dewan Bahasa 5/9",  "postal_code": "50998", "city": "USJ 89q",            "state": "Kuala Lumpur",          "latitude": "3.117134", "longitude": "101.615027"},
    {"store_id": "2",  "store_name": "G Coffee @ Kondominium Putra",  "street": "Jln Yew 6X",              "postal_code": "63826", "city": "Kondominium Putra",  "state": "Selangor Darul Ehsan",  "latitude": "2.959571", "longitude": "101.51772"},
    {"store_id": "3",  "store_name": "G Coffee @ USJ 57W",            "street": "Jalan Bukit Petaling 5/16C","postal_code": "62094","city": "USJ 57W",            "state": "Putrajaya",             "latitude": "2.951038", "longitude": "101.663698"},
    {"store_id": "4",  "store_name": "G Coffee @ Kampung Changkat",   "street": "Jln 6/6A",                "postal_code": "62941", "city": "Kampung Changkat",   "state": "Putrajaya",             "latitude": "2.914594", "longitude": "101.704486"},
    {"store_id": "5",  "store_name": "G Coffee @ Seksyen 21",         "street": "Jalan Anson 4k",          "postal_code": "62595", "city": "Seksyen 21",         "state": "Putrajaya",             "latitude": "2.937599", "longitude": "101.698478"},
    {"store_id": "6",  "store_name": "G Coffee @ Alam Tun Hussein Onn","street": "Jln Pasar Besar 63s",    "postal_code": "63518", "city": "Alam Tun Hussein Onn","state": "Selangor Darul Ehsan",  "latitude": "3.279175", "longitude": "101.784923"},
    {"store_id": "7",  "store_name": "G Coffee @ Damansara Saujana",  "street": "Jln 8/74",                "postal_code": "65438", "city": "Damansara Saujana",  "state": "Selangor Darul Ehsan",  "latitude": "3.22081",  "longitude": "101.58459"},
    {"store_id": "8",  "store_name": "G Coffee @ Bandar Seri Mulia",  "street": "Jalan Wisma Putra",       "postal_code": "58621", "city": "Bandar Seri Mulia",  "state": "Kuala Lumpur",          "latitude": "3.140674", "longitude": "101.706562"},
    {"store_id": "9",  "store_name": "G Coffee @ PJS8",               "street": "Jalan 7/3o",              "postal_code": "62418", "city": "PJS8",               "state": "Putrajaya",             "latitude": "2.952444", "longitude": "101.702623"},
    {"store_id": "10", "store_name": "G Coffee @ Taman Damansara",    "street": "Jln 2",                   "postal_code": "67102", "city": "Taman Damansara",    "state": "Selangor Darul Ehsan",  "latitude": "3.497178", "longitude": "101.595271"},
]

# Struct requerido: store_id, store_name
@dataclass
class StoreInfo:
    store_id: int
    store_name: str

def handle_sigterm(signum, frame):
    print("\n[stores_cleaner] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[stores_cleaner] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# ------------------------------------------------------------
# 1) Obtener información (desde memoria)
# ------------------------------------------------------------
def obtener_informacion() -> List[Dict[str, Any]]:
    print("[stores_cleaner] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)
    rows = [dict(row) for row in STORES_DATA]
    print(f"[stores_cleaner] Leídas {len(rows)} filas desde memoria.", flush=True)
    return rows

# ------------------------------------------------------------
# 2) Procesar información (genera vector de structs)
# ------------------------------------------------------------
def procesar_informacion() -> List[StoreInfo]:
    """
    Construye StoreInfo con: store_id, store_name.
    """
    if STOP_EVENT.is_set():
        return []

    rows = obtener_informacion()
    items: List[StoreInfo] = []
    for r in rows:
        try:
            sid = int(str(r.get("store_id", "0")).strip())
            sname = str(r.get("store_name", "")).strip()
            if not sname:
                continue
            items.append(StoreInfo(store_id=sid, store_name=sname))
        except ValueError:
            continue

    print(f"[stores_cleaner] LIMPIANDO / PROCESANDO INFORMACIÓN… ({len(items)} items)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# ------------------------------------------------------------
# 3) Enviar información (por ahora, imprimir los structs)
# ------------------------------------------------------------
def enviar_informacion(items: List[StoreInfo]) -> None:
    print("[stores_cleaner] MANDANDO INFORMACIÓN…", flush=True)
    for it in items:
        print(f"[stores_cleaner] -> store_id={it.store_id}, store_name={it.store_name}", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers de señales (NO TOCAR)
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[stores_cleaner] Iniciado. PID:", os.getpid(), flush=True)

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

    print("[stores_cleaner] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[stores_cleaner] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

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
SLEEP_SECS = float(os.getenv("CLEANER_SLEEP_SECS", "2"))           # sleep de pasos
POST_CYCLE_SLEEP_SECS = float(os.getenv("POST_CYCLE_SLEEP_SECS", "3"))  # sleep post-ciclo

# Para limpiar pantalla al final del ciclo
def _clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

# --- Dataset en memoria (equivale a .data/menu_items/menu_items.csv) ---
# Columns: item_id,item_name,category,price,is_seasonal,available_from,available_to
MENU_ITEMS_DATA: List[Dict[str, Any]] = [
    {"item_id": "1", "item_name": "Espresso",       "category": "coffee",      "price": "6.0",  "is_seasonal": "False", "available_from": "", "available_to": ""},
    {"item_id": "2", "item_name": "Americano",      "category": "coffee",      "price": "7.0",  "is_seasonal": "False", "available_from": "", "available_to": ""},
    {"item_id": "3", "item_name": "Latte",          "category": "coffee",      "price": "8.0",  "is_seasonal": "False", "available_from": "", "available_to": ""},
    {"item_id": "4", "item_name": "Cappuccino",     "category": "coffee",      "price": "8.0",  "is_seasonal": "False", "available_from": "", "available_to": ""},
    {"item_id": "5", "item_name": "Flat White",     "category": "coffee",      "price": "9.0",  "is_seasonal": "False", "available_from": "", "available_to": ""},
    {"item_id": "6", "item_name": "Mocha",          "category": "coffee",      "price": "9.5",  "is_seasonal": "False", "available_from": "", "available_to": ""},
    {"item_id": "7", "item_name": "Hot Chocolate",  "category": "non-coffee",  "price": "9.0",  "is_seasonal": "False", "available_from": "", "available_to": ""},
    {"item_id": "8", "item_name": "Matcha Latte",   "category": "non-coffee",  "price": "10.0", "is_seasonal": "False", "available_from": "", "available_to": ""},
]

@dataclass
class MenuItem:
    item_id: int
    item_name: str

def handle_sigterm(signum, frame):
    print("\n[menu_cleaner] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[menu_cleaner] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# ------------------------------------------------------------
# 1) Obtener información (desde memoria)
# ------------------------------------------------------------
def obtener_informacion() -> List[Dict[str, Any]]:
    """Devuelve una lista de dicts (una por fila) desde el dataset en memoria."""
    print("[menu_cleaner] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)
    # Podríamos hacer una copia superficial para evitar mutaciones externas
    rows = [dict(row) for row in MENU_ITEMS_DATA]
    print(f"[menu_cleaner] Leídas {len(rows)} filas desde memoria.", flush=True)
    return rows

# ------------------------------------------------------------
# 2) Procesar información (genera vector de structs)
# ------------------------------------------------------------
def procesar_informacion() -> List[MenuItem]:
    """
    Llama a obtener_informacion(), toma las columnas item_id e item_name,
    y devuelve un vector de 'structs' (dataclass MenuItem).
    """
    if STOP_EVENT.is_set():
        return []
    rows = obtener_informacion()
    items: List[MenuItem] = []
    for r in rows:
        try:
            item_id_raw = r.get("item_id")
            item_name = r.get("item_name", "")
            if item_id_raw is None:
                continue
            item_id = int(item_id_raw)
            items.append(MenuItem(item_id=item_id, item_name=item_name))
        except ValueError:
            # Si item_id no es convertible a int, lo salteamos
            continue
    print(f"[menu_cleaner] LIMPIANDO / PROCESANDO INFORMACIÓN… ({len(items)} items)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# ------------------------------------------------------------
# 3) Enviar información (por ahora, imprimir los structs)
# ------------------------------------------------------------
def enviar_informacion(items: List[MenuItem]) -> None:
    """En el futuro enviará por middleware. Por ahora, imprime los structs de forma clara."""
    print("[menu_cleaner] MANDANDO INFORMACIÓN…", flush=True)
    for it in items:
        print(f"[menu_cleaner] -> item_id={it.item_id}, item_name={it.item_name}", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers de señales (NO TOCAR)
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[menu_cleaner] Iniciado. PID:", os.getpid(), flush=True)

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

    print("[menu_cleaner] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[menu_cleaner] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

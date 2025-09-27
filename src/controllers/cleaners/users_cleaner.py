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

# --- Dataset en memoria (equivale a .data/users/users.csv) ---
# Columns: user_id,gender,birthdate,registered_at
USERS_DATA: List[Dict[str, Any]] = [
    {"user_id": "17585", "gender": "female", "birthdate": "2003-01-06", "registered_at": "2024-01-02 10:42:29"},
    {"user_id": "17586", "gender": "female", "birthdate": "2002-03-29", "registered_at": "2024-01-02 10:42:50"},
    {"user_id": "17587", "gender": "male",   "birthdate": "1979-05-28", "registered_at": "2024-01-02 10:42:55"},
    {"user_id": "17588", "gender": "female", "birthdate": "1979-09-14", "registered_at": "2024-01-02 10:43:37"},
    {"user_id": "17589", "gender": "female", "birthdate": "1981-03-22", "registered_at": "2024-01-02 10:46:17"},
    {"user_id": "17590", "gender": "male",   "birthdate": "1986-07-20", "registered_at": "2024-01-02 10:46:41"},
    {"user_id": "17591", "gender": "female", "birthdate": "1997-05-20", "registered_at": "2024-01-02 10:48:52"},
    {"user_id": "17592", "gender": "male",   "birthdate": "2008-07-05", "registered_at": "2024-01-02 10:50:38"},
    {"user_id": "17593", "gender": "female", "birthdate": "1986-02-20", "registered_at": "2024-01-02 10:54:15"},
    {"user_id": "17594", "gender": "female", "birthdate": "2005-06-23", "registered_at": "2024-01-02 10:59:33"},
]

# Struct requerido: user_id, birthdate
@dataclass
class UserInfo:
    user_id: int
    birthdate: str  # lo dejamos como string legible (podemos parsear más adelante)

def handle_sigterm(signum, frame):
    print("\n[user_cleaner] SIGTERM recibido. Iniciando cierre…", flush=True)
    STOP_EVENT.set()

def handle_sigint(signum, frame):
    print("\n[user_cleaner] SIGINT recibido (Ctrl+C). Cerrando…", flush=True)
    STOP_EVENT.set()

# ------------------------------------------------------------
# 1) Obtener información (desde memoria)
# ------------------------------------------------------------
def obtener_informacion() -> List[Dict[str, Any]]:
    print("[user_cleaner] OBTENIENDO INFORMACIÓN… (fuente: memoria)", flush=True)
    rows = [dict(row) for row in USERS_DATA]
    print(f"[user_cleaner] Leídas {len(rows)} filas desde memoria.", flush=True)
    return rows

# ------------------------------------------------------------
# 2) Procesar información (genera vector de structs)
# ------------------------------------------------------------
def procesar_informacion() -> List[UserInfo]:
    """
    Construye UserInfo con: user_id, birthdate.
    """
    if STOP_EVENT.is_set():
        return []

    rows = obtener_informacion()
    items: List[UserInfo] = []
    for r in rows:
        try:
            uid = int(str(r.get("user_id", "0")).strip())
            birthdate = str(r.get("birthdate", "")).strip()
            if not birthdate:
                continue
            items.append(UserInfo(user_id=uid, birthdate=birthdate))
        except ValueError:
            continue

    print(f"[user_cleaner] LIMPIANDO / PROCESANDO INFORMACIÓN… ({len(items)} items)", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)
    return items

# ------------------------------------------------------------
# 3) Enviar información (por ahora, imprimir los structs)
# ------------------------------------------------------------
def enviar_informacion(items: List[UserInfo]) -> None:
    print("[user_cleaner] MANDANDO INFORMACIÓN…", flush=True)
    for it in items:
        print(f"[user_cleaner] -> user_id={it.user_id}, birthdate={it.birthdate}", flush=True)
    if SLEEP_SECS > 0:
        time.sleep(SLEEP_SECS)

def main():
    # Handlers de señales (NO TOCAR)
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigint)

    print("[user_cleaner] Iniciado. PID:", os.getpid(), flush=True)

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

    print("[user_cleaner] Finalizando de forma ordenada. ¡Listo!", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[user_cleaner] Error no controlado: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

#!/usr/bin/env python3
import os
import re
import sys

LOG_DIR = "logs"

session_ids = set()

for fname in os.listdir(LOG_DIR):
    if not re.match(r"client_\d+\.log$", fname):
        continue

    path = os.path.join(LOG_DIR, fname)
    with open(path, "r") as f:
        content = f.read()

    ids = re.findall(r"session_id:\s*([a-zA-Z0-9_-]+)", content)
    if ids:
        print(f"🧩 {fname}: encontrado(s) session_id(s) {', '.join(sorted(set(ids)))}")
        session_ids.update(ids)

if not session_ids:
    print("⚠️ No se encontraron session_id en los logs de clientes.")
    sys.exit(0)

session_ids = sorted(session_ids)
print(f"\n🔍 Se encontraron {len(session_ids)} session_id(s) únicos: {', '.join(session_ids)}")

exit_code = 0
for fname in os.listdir(LOG_DIR):
    if not fname.endswith(".log"):
        continue
    if re.match(r"client_\d+\.log$", fname):
        continue 
    if fname == "server.log":
        continue 

    path = os.path.join(LOG_DIR, fname)
    service_name = fname[:-4]

    is_cleaner = bool(re.match(r".*_cleaner_\d+\.log$", fname))

    with open(path, "r") as f:
        data = f.read()

    print(f"\n🔸 Servicio: {service_name}")
    missing_received = []
    missing_sent = []

    for sid in session_ids:
        if is_cleaner:
            pattern_sent = rf"action:\s*eof_sent\s*\|\s*result:\s*\w+\s*\|\s*session_id:\s*{sid}"
            if not re.search(pattern_sent, data):
                missing_sent.append(sid)
            continue

        pattern_received = rf"action:\s*all_eofs_received\s*\|\s*result:\s*\w+\s*\|\s*session_id:\s*{sid}"
        pattern_sent = rf"action:\s*eof_sent\s*\|\s*result:\s*\w+\s*\|\s*session_id:\s*{sid}"

        if not re.search(pattern_received, data):
            missing_received.append(sid)
        if not re.search(pattern_sent, data):
            missing_sent.append(sid)

    if missing_received or missing_sent:
        exit_code = 1
        if missing_received:
            print(f"  ❌ Faltan all_eofs_received para {len(missing_received)} session_id(s): {', '.join(missing_received)}")
        if missing_sent:
            print(f"  ❌ Faltan eof_sent para {len(missing_sent)} session_id(s): {', '.join(missing_sent)}")
    else:
        if is_cleaner:
            print("  ✅ Todos los EOFs enviados correctamente (cleaner)")
        else:
            print("  ✅ Todos los EOFs recibidos y enviados correctamente")

sys.exit(exit_code)

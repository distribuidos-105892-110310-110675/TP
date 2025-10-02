#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compara archivos "esperados" vs "actuales" para Q1X, Q3X, Q4X, Q21 y Q22.

- Para Q1X: Solo verifica que la CANTIDAD de filas coincida.
- Para Q3X, Q4X, Q21, Q22: verifica que el CONTENIDO sea el mismo
  independientemente del orden de las filas (multiconjunto de líneas).

Imprime un resultado por cada archivo y un resumen final.
Devuelve exit code 0 si todo está OK; 1 si hubo al menos un error.

Uso:
    python compare_results.py --expected /ruta/a/esperados --actual /ruta/a/actuales

Opciones útiles:
    --suffix "_result.txt"     Sufijo que deben compartir los archivos a comparar.
    --include-empty-lines      Cuenta/considera también las líneas vacías (por defecto se ignoran).
    --case-sensitive           Compara con sensibilidad a mayúsculas/minúsculas (por defecto normaliza).
    --encoding "utf-8"         Encoding de lectura de archivos.
"""

import argparse
import os
import sys
from collections import Counter

DEFAULT_TAGS = {
    "Q1X": "count_only",
    "Q3X": "multiset",
    "Q4X": "multiset",
    "Q21": "multiset",
    "Q22": "multiset",
}

def norm_line(s: str, case_sensitive: bool) -> str:
    s = s.rstrip("\n\r")
    if not case_sensitive:
        return s.strip().lower()
    return s.strip()

def read_lines(path: str, encoding: str, include_empty: bool, case_sensitive: bool):
    try:
        with open(path, "r", encoding=encoding, errors="replace") as f:
            lines = [norm_line(l, case_sensitive) for l in f]
            if not include_empty:
                lines = [l for l in lines if l != ""]
            return lines
    except FileNotFoundError:
        return None

def detect_tag(filename: str):
    for tag in DEFAULT_TAGS.keys():
        if tag in filename:
            return tag
    return None

def compare_count_only(exp_path, act_path, encoding, include_empty, case_sensitive):
    exp = read_lines(exp_path, encoding, include_empty, case_sensitive)
    act = read_lines(act_path, encoding, include_empty, case_sensitive)
    if exp is None:
        return False, f"No se encontró el archivo esperado: {exp_path}"
    if act is None:
        return False, f"No se encontró el archivo actual: {act_path}"
    return (len(exp) == len(act),
            f"filas esperadas={len(exp)}, actuales={len(act)}")

def compare_multiset(exp_path, act_path, encoding, include_empty, case_sensitive, max_diff_examples=5):
    exp = read_lines(exp_path, encoding, include_empty, case_sensitive)
    act = read_lines(act_path, encoding, include_empty, case_sensitive)
    if exp is None:
        return False, f"No se encontró el archivo esperado: {exp_path}"
    if act is None:
        return False, f"No se encontró el archivo actual: {act_path}"

    c_exp = Counter(exp)
    c_act = Counter(act)
    if c_exp == c_act:
        return True, f"{len(exp)} filas (mismo contenido, orden ignorado)"
    else:
        # Construir un pequeño diff legible
        missing = list((c_exp - c_act).elements())
        extra = list((c_act - c_exp).elements())
        msg_parts = []
        if missing:
            msg_parts.append(f"faltan {len(missing)} línea(s) en 'actual' (p.ej.: {', '.join(repr(x) for x in missing[:max_diff_examples])}{'...' if len(missing)>max_diff_examples else ''})")
        if extra:
            msg_parts.append(f"sobra(n) {len(extra)} línea(s) en 'actual' (p.ej.: {', '.join(repr(x) for x in extra[:max_diff_examples])}{'...' if len(extra)>max_diff_examples else ''})")
        return False, "; ".join(msg_parts) if msg_parts else "Los multisets difieren."

def main():
    ap = argparse.ArgumentParser(description="Comparador de resultados por archivo (Q1X/Q3X/Q4X/Q21/Q22).")
    ap.add_argument("--expected", required=True, help="Carpeta con archivos esperados (los que me enviaste).")
    ap.add_argument("--actual", required=True, help="Carpeta con archivos actuales (salidas a validar).")
    ap.add_argument("--suffix", default="_result.txt", help="Sufijo común de los archivos a comparar. (default: %(default)s)")
    ap.add_argument("--include-empty-lines", action="store_true", help="Considera también las líneas vacías.")
    ap.add_argument("--case-sensitive", action="store_true", help="Comparaciones sensibles a mayúsculas/minúsculas.")
    ap.add_argument("--encoding", default="utf-8", help="Encoding de lectura. (default: %(default)s)")

    args = ap.parse_args()

    if not os.path.isdir(args.expected):
        print(f"[ERROR] Carpeta 'expected' inválida: {args.expected}")
        sys.exit(1)
    if not os.path.isdir(args.actual):
        print(f"[ERROR] Carpeta 'actual' inválida: {args.actual}")
        sys.exit(1)

    expected_files = [f for f in os.listdir(args.expected) if f.endswith(args.suffix)]
    if not expected_files:
        print(f"[ADVERTENCIA] No se encontraron archivos con sufijo {args.suffix} en {args.expected}")
        sys.exit(1)

    print(f"-> Comparando {len(expected_files)} archivo(s) usando sufijo '{args.suffix}'\n")

    all_ok = True
    for fname in sorted(expected_files):
        expected_path = os.path.join(args.expected, fname)
        actual_path = os.path.join(args.actual, fname)

        tag = detect_tag(fname)
        if tag is None:
            print(f"[SKIP] {fname}: no se reconoce ninguna etiqueta conocida (Q1X/Q3X/Q4X/Q21/Q22).")
            continue

        mode = DEFAULT_TAGS[tag]
        print(f"[INFO] {fname} ({tag}) -> modo: {'conteo' if mode=='count_only' else 'multiconjunto'}")

        if mode == "count_only":
            ok, detail = compare_count_only(expected_path, actual_path, args.encoding, args.include_empty_lines, args.case_sensitive)
        else:
            ok, detail = compare_multiset(expected_path, actual_path, args.encoding, args.include_empty_lines, args.case_sensitive)

        status = "OK" if ok else "ERROR"
        print(f"[{status}] {fname}: {detail}\n")
        all_ok = all_ok and ok

    print("==== RESUMEN ====")
    print("Todo OK ✅" if all_ok else "Hay diferencias ❌")
    sys.exit(0 if all_ok else 1)

if __name__ == "__main__":
    main()

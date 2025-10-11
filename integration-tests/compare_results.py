#!/usr/bin/env python3
# -- coding: utf-8 --
"""
Comparador de resultados para Q1X, Q21, Q22, Q3X y Q4X.

Reglas:
- Q1X: solo verifica que la CANTIDAD de filas coincida (conteo).
- Q21, Q22, Q3X: verifica que el CONTENIDO sea el mismo ignorando el orden (multiconjunto de líneas).
- Q4X: validación especial por cafetería:
    * Igual cantidad TOTAL de líneas.
    * Igual conjunto de cafeterías.
    * Para cada cafetería: igual cantidad de líneas.
    * Para cada cafetería: iguales cantidades de compras (tercer campo) en sus como máximo 3 filas,
      ignorando el orden y sin importar quiénes sean los clientes (desempates aceptados).
      El contenido (fechas/personas) no importa; sólo importan los contadores por top-k.

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
from collections import Counter, defaultdict
from typing import List, Tuple, Dict, Optional

# Mapeo de modos por etiqueta detectada en el nombre del archivo
DEFAULT_TAGS = {
    "Q1X": "count_only",     # Cuenta filas
    "Q3X": "multiset",       # Contenido, orden ignorado
    "Q4X": "q4x_special",    # Validación especial por cafetería
    "Q21": "multiset",
    "Q22": "multiset",       # <-- ahora multiset (antes era count_only)
}

def norm_line(s: str, case_sensitive: bool) -> str:
    s = s.rstrip("\n\r")
    if not case_sensitive:
        return s.strip().lower()
    return s.strip()

def read_lines(path: str, encoding: str, include_empty: bool, case_sensitive: bool) -> Optional[List[str]]:
    try:
        with open(path, "r", encoding=encoding, errors="replace") as f:
            lines = [norm_line(l, case_sensitive) for l in f]
            if not include_empty:
                lines = [l for l in lines if l != ""]
            return lines
    except FileNotFoundError:
        return None

def detect_tag(filename: str) -> Optional[str]:
    for tag in DEFAULT_TAGS.keys():
        if tag in filename:
            return tag
    return None

def compare_count_only(exp_path, act_path, encoding, include_empty, case_sensitive) -> Tuple[bool, str]:
    exp = read_lines(exp_path, encoding, include_empty, case_sensitive)
    act = read_lines(act_path, encoding, include_empty, case_sensitive)
    if exp is None:
        return False, f"No se encontró el archivo esperado: {exp_path}"
    if act is None:
        return False, f"No se encontró el archivo actual: {act_path}"
    return (len(exp) == len(act),
            f"filas esperadas={len(exp)}, actuales={len(act)}")

def compare_multiset(exp_path, act_path, encoding, include_empty, case_sensitive, max_diff_examples=5) -> Tuple[bool, str]:
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
        missing = list((c_exp - c_act).elements())
        extra = list((c_act - c_exp).elements())
        msg_parts = []
        if missing:
            msg_parts.append(
                f"faltan {len(missing)} línea(s) en 'actual' (p.ej.: {', '.join(repr(x) for x in missing[:max_diff_examples])}{'...' if len(missing)>max_diff_examples else ''})"
            )
        if extra:
            msg_parts.append(
                f"sobra(n) {len(extra)} línea(s) en 'actual' (p.ej.: {', '.join(repr(x) for x in extra[:max_diff_examples])}{'...' if len(extra)>max_diff_examples else ''})"
            )
        return False, "; ".join(msg_parts) if msg_parts else "Los multisets difieren."

# --------------------------
# Comparación especial Q4X
# --------------------------

def _parse_q4x_line(line: str) -> Optional[Tuple[str, str, int]]:
    """
    Formato esperado (campos separados por coma):
      cafe,fecha,conteo
    Se usa rsplit con max 2 separaciones por si el nombre de la cafetería tuviera comas.
    """
    parts = line.rsplit(",", 2)
    if len(parts) != 3:
        return None
    cafe = parts[0].strip()
    fecha = parts[1].strip()  # no se usa para comparar
    try:
        count = int(parts[2].strip())
    except ValueError:
        return None
    return cafe, fecha, count

def _group_counts_by_cafe(lines: List[str]) -> Tuple[Dict[str, List[int]], List[str]]:
    """
    Devuelve:
      - dict cafe -> lista de contadores (int)
      - lista de líneas malformadas (para diagnóstico)
    """
    groups: Dict[str, List[int]] = defaultdict(list)
    bad: List[str] = []
    for ln in lines:
        parsed = _parse_q4x_line(ln)
        if parsed is None:
            bad.append(ln)
            continue
        cafe, _fecha, cnt = parsed
        groups[cafe].append(cnt)
    return groups, bad

def compare_q4x(exp_path, act_path, encoding, include_empty, case_sensitive, max_examples=5) -> Tuple[bool, str]:
    """
    Reglas Q4X:
      - Igual cantidad total de líneas.
      - Igual conjunto de cafeterías.
      - Para cada cafetería:
          * Igual cantidad de líneas (como máximo 3).
          * Igual multiset de conteos (tercer campo), ignorando orden y sin importar identidad/fecha.
    """
    exp = read_lines(exp_path, encoding, include_empty, case_sensitive)
    act = read_lines(act_path, encoding, include_empty, case_sensitive)
    if exp is None:
        return False, f"No se encontró el archivo esperado: {exp_path}"
    if act is None:
        return False, f"No se encontró el archivo actual: {act_path}"

    # 1) Cantidad total de líneas
    if len(exp) != len(act):
        return False, f"Cantidad total de filas difiere: esperadas={len(exp)}, actuales={len(act)}"

    # 2) Parseo y agrupación por cafetería
    exp_groups, exp_bad = _group_counts_by_cafe(exp)
    act_groups, act_bad = _group_counts_by_cafe(act)

    if exp_bad:
        return False, f"Archivos 'esperados' con líneas mal formadas (p.ej.: {', '.join(repr(x) for x in exp_bad[:max_examples])}{'...' if len(exp_bad)>max_examples else ''})"
    if act_bad:
        return False, f"Archivos 'actuales' con líneas mal formadas (p.ej.: {', '.join(repr(x) for x in act_bad[:max_examples])}{'...' if len(act_bad)>max_examples else ''})"

    # 3) Mismo conjunto de cafeterías
    cafes_exp = set(exp_groups.keys())
    cafes_act = set(act_groups.keys())
    if cafes_exp != cafes_act:
        faltan = cafes_exp - cafes_act
        sobran = cafes_act - cafes_exp
        partes = []
        if faltan:
            partes.append(f"faltan cafeterías en 'actual': {', '.join(sorted(faltan))}")
        if sobran:
            partes.append(f"sobran cafeterías en 'actual': {', '.join(sorted(sobran))}")
        return False, "; ".join(partes) if partes else "Diferencia en el conjunto de cafeterías."

    # 4) Por cafetería: misma cantidad de líneas y mismo multiset de conteos
    difs = []
    for cafe in sorted(cafes_exp):
        exp_counts = exp_groups[cafe]
        act_counts = act_groups[cafe]

        if len(exp_counts) != len(act_counts):
            difs.append(f"[{cafe}] cantidad de filas difiere (esperadas={len(exp_counts)}, actuales={len(act_counts)})")
            continue

        if Counter(exp_counts) != Counter(act_counts):
            # Mostrar ejemplo de diferencias
            missing = list((Counter(exp_counts) - Counter(act_counts)).elements())
            extra = list((Counter(act_counts) - Counter(exp_counts)).elements())
            msg = f"[{cafe}] difieren los conteos: "
            parts = []
            if missing:
                parts.append(f"faltan en 'actual': {missing[:max_examples]}{'...' if len(missing)>max_examples else ''}")
            if extra:
                parts.append(f"sobra(n) en 'actual': {extra[:max_examples]}{'...' if len(extra)>max_examples else ''}")
            difs.append(msg + "; ".join(parts))

    if difs:
        return False, " | ".join(difs)

    # OK
    total_cafes = len(cafes_exp)
    return True, f"{len(exp)} filas, {total_cafes} cafeterías (misma distribución de top-k por conteo)"

# --------------------------
# Programa principal
# --------------------------

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
        modo_str = {"count_only": "conteo",
                    "multiset": "multiconjunto (orden ignorado)",
                    "q4x_special": "validación especial por cafetería (top-k por conteo)"}[mode]
        print(f"[INFO] {fname} ({tag}) -> modo: {modo_str}")

        if mode == "count_only":
            ok, detail = compare_count_only(expected_path, actual_path, args.encoding, args.include_empty_lines, args.case_sensitive)
        elif mode == "multiset":
            ok, detail = compare_multiset(expected_path, actual_path, args.encoding, args.include_empty_lines, args.case_sensitive)
        elif mode == "q4x_special":
            ok, detail = compare_q4x(expected_path, actual_path, args.encoding, args.include_empty_lines, args.case_sensitive)
        else:
            ok, detail = False, "Modo de comparación desconocido."

        status = "OK" if ok else "ERROR"
        print(f"[{status}] {fname}: {detail}\n")
        all_ok = all_ok and ok

    print("==== RESUMEN ====")
    print("Todo OK ✅" if all_ok else "Hay diferencias ❌")
    sys.exit(0 if all_ok else 1)

if __name__ == "__main__":
    main()

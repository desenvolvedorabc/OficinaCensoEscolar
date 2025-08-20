#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
convertutf8.py — Converte um CSV para UTF-8 preservando a estrutura (sem reparsear).
Uso:
  python convertutf8.py entrada.csv
  python convertutf8.py entrada.csv saida.csv
  python convertutf8.py entrada.csv -o saida.csv
  python convertutf8.py entrada.csv --encoding latin-1 --errors replace
"""
from pathlib import Path
import argparse, sys

def quick_detect_encoding(path: Path) -> str:
    # Tentativas rápidas e seguras (amostra ~1MB)
    candidates = ("utf-8", "utf-8-sig", "latin-1", "cp1252")
    raw = path.read_bytes()[:1_000_000]
    for enc in candidates:
        try:
            raw.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    # Tentativa com charset-normalizer (opcional)
    try:
        from charset_normalizer import from_bytes
        best = from_bytes(raw).best()
        if best and best.encoding:
            return best.encoding
    except Exception:
        pass
    return "latin-1"  # fallback conservador

def main():
    ap = argparse.ArgumentParser(description="Converte CSV para UTF-8 preservando estrutura.")
    ap.add_argument("input", type=Path, help="CSV de entrada")
    ap.add_argument("output_pos", nargs="?", type=Path, help="CSV de saída (opcional)")
    ap.add_argument("-o", "--output", type=Path, help="CSV de saída (opcional)")
    ap.add_argument("--encoding", help="Encoding de entrada (ex.: latin-1, cp1252, utf-8-sig)")
    ap.add_argument("--errors", default="strict", choices=["strict","replace","ignore"],
                    help="Tratamento de caracteres inválidos (padrão: strict)")
    args = ap.parse_args()

    if not args.input.exists():
        print(f"[ERRO] Arquivo não encontrado: {args.input}", file=sys.stderr)
        sys.exit(1)

    out = args.output_pos or args.output or args.input.with_name(args.input.stem + "_utf8.csv")
    src_enc = args.encoding or quick_detect_encoding(args.input)
    print(f"[INFO] Encoding de entrada: {src_enc}")
    print(f"[INFO] Saída: {out}")

    # Copia linha a linha; newline="" preserva as quebras como no arquivo de origem
    with args.input.open("r", encoding=src_enc, errors=args.errors, newline="") as fin, \
         out.open("w", encoding="utf-8", errors="strict", newline="") as fout:
        for line in fin:
            fout.write(line)

    print("[OK] Conversão concluída.")

if __name__ == "__main__":
    main()

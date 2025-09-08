#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL → Mozc 辞書 TSV 生成スクリプト

・words / pos_codes テーブルを結合して Mozc 用 5 列 TSV を出力
・--output/-o でファイル指定、未指定時は標準出力
・DB 接続オプションは共通 CLI (+ 環境変数)
"""

import argparse
import sys
from psycopg2 import Error

from db_utils import add_common_args, get_db_connection_from_args, get_dict_cursor


def generate_tsv(conn, out_fp):
    cur = get_dict_cursor(conn)
    generated = set()
    count = 0

    try:
        cur.execute(
            """
        SELECT w.reading, w.word, p.name AS pos_name
          FROM words w
          JOIN pos_codes p ON p.code = w.pos_code
         ORDER BY w.reading, w.word
        """
        )

        for row in cur:
            reading, word, pos_name = row["reading"], row["word"], row["pos_name"]

            if pos_name == "固有名詞":
                line = f"{reading}\t1920\t1920\t4001\t{word}"
            elif pos_name == "普通名詞":
                line = f"{reading}\t1851\t1851\t4000\t{word}"
            else:
                print(f"未対応品詞スキップ: {row}", file=sys.stderr)
                continue

            if line not in generated:
                print(line, file=out_fp)
                generated.add(line)
                count += 1

    finally:
        cur.close()

    print(f"✔ {count} 行生成 (unique)", file=sys.stderr)


def build_parser():
    p = argparse.ArgumentParser(description="Mozc 辞書 TSV 生成")
    p.add_argument(
        "-o", "--output", metavar="PATH", help="出力 TSV ファイル（未指定時は標準出力）"
    )
    add_common_args(p)
    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    conn = None
    out_fp = None
    try:
        conn = get_db_connection_from_args(args)

        if args.output:
            out_fp = open(args.output, "w", encoding="utf-8", newline="\n")
            print(f"ファイル出力: {args.output}", file=sys.stderr)
        else:
            out_fp = sys.stdout
            print("標準出力へ出力", file=sys.stderr)

        generate_tsv(conn, out_fp)

    except (Error, OSError, ValueError) as e:
        print(f"✖ エラー: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if out_fp and out_fp is not sys.stdout:
            out_fp.close()
        if conn and not conn.closed:
            conn.close()
            print("接続を閉じました", file=sys.stderr)


if __name__ == "__main__":
    main()


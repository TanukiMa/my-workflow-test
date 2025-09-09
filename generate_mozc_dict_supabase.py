#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase → Mozc 辞書 TSV 生成スクリプト

・words / pos_codes テーブルを結合して Mozc 用 5 列 TSV を出力
・--output/-o でファイル指定、未指定時は標準出力
・Supabase 接続オプションは共通 CLI (+ 環境変数)
"""

import argparse
import sys
from supabase import create_client, Client

from supabase_utils import add_common_args, get_supabase_client_from_args

def generate_tsv(supabase: Client, out_fp):
    generated = set()
    count = 0

    try:
        # Supabase RPC を使用
        try:
            # RPC関数を使う場合（推奨）
            response = supabase.rpc('get_words_with_pos', {}).execute()
        except Exception:
            # RPC関数が存在しない場合は直接テーブルクエリ
            response = supabase.table('words') \
                .select('reading, word, pos_codes(name)') \
                .order('reading', ascending=True) \
                .order('word', ascending=True) \
                .execute()

        if not response.
            print("データが見つかりませんでした", file=sys.stderr)
            return

        for row in response.
            reading = row.get('reading', '')
            word = row.get('word', '')
            # RPC関数の場合とテーブル直接クエリの場合で処理を分岐
            pos_name = row.get('pos_name') or (row.get('pos_codes', {}).get('name', '') if row.get('pos_codes') else '')

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

        if count == 0:
            print("⚠ 処理可能なデータがありませんでした", file=sys.stderr)

    except Exception as e:
        print(f"データ取得エラー: {e}", file=sys.stderr)
        raise
    finally:
        if out_fp and out_fp is not sys.stdout:
            out_fp.close()
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

    supabase = None
    out_fp = None
    try:
        supabase = get_supabase_client_from_args(args)

        if args.output:
            out_fp = open(args.output, "w", encoding="utf-8", newline="\n")
            print(f"ファイル出力: {args.output}", file=sys.stderr)
        else:
            out_fp = sys.stdout
            print("標準出力へ出力", file=sys.stderr)

        generate_tsv(supabase, out_fp)

    except Exception as e:
        print(f"✖ エラー: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if out_fp and out_fp is not sys.stdout:
            out_fp.close()
        print("処理完了", file=sys.stderr)


if __name__ == "__main__":
    main()


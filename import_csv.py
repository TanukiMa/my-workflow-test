#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV → PostgreSQL インポータ

・--host/-H, --port/-P, --database/-d, --user/-U, --password/-W で接続先を指定
・指定なしは環境変数 (.env も可) を参照
"""

import csv
import argparse
import sys
from psycopg2 import Error

from db_utils import (
    add_common_args,
    get_db_connection_from_args,
    initialize_database,
    get_dict_cursor,
)

# ----------------------------------------------------------------------
# 1. 補助関数
# ----------------------------------------------------------------------
def get_or_create_pos_code(cur, pos_name: str) -> int:
    cur.execute("SELECT code FROM pos_codes WHERE name = %s", (pos_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    raise ValueError(f"未知の品詞: {pos_name} (pos_codes に登録してください)")


def get_or_create_attr_id(cur, conn, attr_name: str) -> int:
    cur.execute("SELECT id FROM attr_codes WHERE name = %s", (attr_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    try:
        cur.execute(
            "INSERT INTO attr_codes (name) VALUES (%s) RETURNING id", (attr_name,)
        )
        conn.commit()
        return cur.fetchone()[0]
    except Error:
        conn.rollback()
        # 競合時は再取得
        cur.execute("SELECT id FROM attr_codes WHERE name = %s", (attr_name,))
        row = cur.fetchone()
        if row:
            return row[0]
        raise


def import_csv(conn, csv_path: str):
    cur = get_dict_cursor(conn)

    inserted = updated = skipped = errors = 0

    try:
        with open(csv_path, newline="", encoding="utf-8") as fp:
            reader = csv.reader(fp)
            for row_num, row in enumerate(reader, start=1):
                if len(row) < 5:
                    print(f"行{row_num}: 列不足でスキップ {row}", file=sys.stderr)
                    skipped += 1
                    continue

                reading, word, pos_name, attr_name, collocation = row

                try:
                    pos_code = get_or_create_pos_code(cur, pos_name)
                    attr_id = get_or_create_attr_id(cur, conn, attr_name)

                    cur.execute(
                        """
                    INSERT INTO words (reading, word, pos_code, attr_id, collocation)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (reading, word, pos_code)
                      DO UPDATE SET
                        collocation = EXCLUDED.collocation,
                        attr_id    = EXCLUDED.attr_id,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                        (reading, word, pos_code, attr_id, collocation),
                    )

                    # INSERT/UPDATE 判定
                    cur.execute(
                        "SELECT xmin = 0 AS inserted FROM words WHERE reading=%s AND word=%s AND pos_code=%s",
                        (reading, word, pos_code),
                    )
                    if cur.fetchone()[0]:
                        inserted += 1
                    else:
                        updated += 1

                except ValueError as ve:
                    print(f"行{row_num}: {ve}", file=sys.stderr)
                    skipped += 1
                except Error as e:
                    print(f"行{row_num}: DB エラー {e}", file=sys.stderr)
                    conn.rollback()
                    errors += 1

        conn.commit()
        print(
            f"✔ インポート完了: inserted={inserted}, updated={updated}, skipped={skipped}, errors={errors}",
            file=sys.stderr,
        )
    finally:
        cur.close()


# ----------------------------------------------------------------------
# 2. CLI
# ----------------------------------------------------------------------
def build_parser():
    p = argparse.ArgumentParser(description="医学辞書 CSV を PostgreSQL にインポート")
    p.add_argument("csv_file", help="CSV ファイルパス")
    add_common_args(p)
    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    conn = None
    try:
        conn = get_db_connection_from_args(args)
        initialize_database(conn)
        import_csv(conn, args.csv_file)
    except Exception as e:
        print(f"✖ エラー: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn and not conn.closed:
            conn.close()
            print("接続を閉じました", file=sys.stderr)


if __name__ == "__main__":
    main()


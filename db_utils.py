#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL ユーティリティモジュール

・CLI オプション / 環境変数を吸収して接続情報を生成
・辞書カーソル／リトライ付きクエリ実行
・初期化（テーブル＋基本データ投入）も提供
"""

import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Error
import psycopg2.extras

# .env（プロジェクトルート等に配置）を読み込み
load_dotenv()


# ----------------------------------------------------------------------
# 1. 接続情報関連
# ----------------------------------------------------------------------
def _env(key: str, default=None):
    """環境変数取得（空文字も未設定とみなす）"""
    val = os.getenv(key)
    return val if val else default


def get_env_config() -> dict:
    """環境変数由来の接続設定を辞書で返す"""
    return {
        "host": _env("PG_HOST", "localhost"),
        "port": int(_env("PG_PORT", 5432)),
        "database": _env("PG_DATABASE"),
        "user": _env("PG_USER"),
        "password": _env("PG_PASSWORD"),
    }

def get_db_connection_from_args(args):
    host = args.host or os.environ.get("PG_HOST")
    port = args.port or os.environ.get("PG_PORT")
    db = args.database or os.environ.get("PG_DATABASE")
    user = args.user or os.environ.get("PG_USER")
    password = args.password or os.environ.get("PG_PASSWORD")

def add_common_args(parser):
    """
    argparse.ArgumentParser に共通 DB オプションを追加

    -H / --host       : ホスト名
    -P / --port       : ポート番号
    -d / --database   : データベース名
    -U / --user       : ユーザー
    -W / --password   : パスワード
    """
    parser.add_argument("-H", "--host", help="データベースホスト (PG_HOST)")
    parser.add_argument("-P", "--port", type=int, help="データベースポート (PG_PORT)")
    parser.add_argument("-d", "--database", help="データベース名 (PG_DATABASE)")
    parser.add_argument("-U", "--user", help="ユーザー名 (PG_USER)")
    parser.add_argument("-W", "--password", help="パスワード (PG_PASSWORD)")
    return parser


def _merge_cli_env(args) -> dict:
    """Namespace と環境変数をマージして最終的な接続設定を作成"""
    cfg = get_env_config()

    if getattr(args, "host", None):
        cfg["host"] = args.host
    if getattr(args, "port", None):
        cfg["port"] = args.port
    if getattr(args, "database", None):
        cfg["database"] = args.database
    if getattr(args, "user", None):
        cfg["user"] = args.user
    if getattr(args, "password", None):
        cfg["password"] = args.password

    # 空文字対策
    for k, v in list(cfg.items()):
        if v == "":
            cfg[k] = None

    return cfg


def get_db_connection_from_args(args, *, cursor_factory=None):
    """
    CLI / 環境変数の情報を用いて psycopg2.connect() を実行

    cursor_factory には psycopg2.extras.DictCursor などを指定可。
    """
    cfg = _merge_cli_env(args)

    # dbname が無いと psycopg2 が失敗するため明示チェック
    if not cfg["database"]:
        raise ValueError(
            "データベース名が指定されていません（--database または PG_DATABASE）"
        )

    try:
        return psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
            cursor_factory=cursor_factory,
        )
    except Error as e:
        raise RuntimeError(f"PostgreSQL 接続失敗: {e}") from e


def get_db_connection(*, cursor_factory=None):
    """環境変数ベースのみで接続（スクリプト外から直接使う場合用）"""
    class _Dummy:  # 環境変数だけを使うためのダミー Namespace
        pass

    return get_db_connection_from_args(_Dummy(), cursor_factory=cursor_factory)


def get_dict_cursor(conn):
    """DictRow を返すカーソル"""
    return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


# ----------------------------------------------------------------------
# 2. 初期化処理
# ----------------------------------------------------------------------
def initialize_database(conn):
    """テーブル作成 & 基本データ挿入（冪等）"""
    cur = conn.cursor()
    try:
        # pos_codes
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS pos_codes (
            code SERIAL PRIMARY KEY,
            name VARCHAR(50) UNIQUE NOT NULL
        )
        """
        )

        # attr_codes
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS attr_codes (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE NOT NULL
        )
        """
        )

        # words
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS words (
            id SERIAL PRIMARY KEY,
            reading     VARCHAR(255) NOT NULL,
            word        VARCHAR(255) NOT NULL,
            pos_code    INTEGER REFERENCES pos_codes(code),
            attr_id     INTEGER REFERENCES attr_codes(id),
            collocation TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(reading, word, pos_code)
        )
        """
        )

        # trigger function
        cur.execute(
            """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        )

        # trigger
        cur.execute(
            """
        DROP TRIGGER IF EXISTS update_words_updated_at ON words;
        CREATE TRIGGER update_words_updated_at
          BEFORE UPDATE ON words
          FOR EACH ROW
          EXECUTE FUNCTION update_updated_at_column();
        """
        )

        # 基本品詞
        cur.execute(
            """
        INSERT INTO pos_codes (name) VALUES ('固有名詞'), ('普通名詞')
        ON CONFLICT (name) DO NOTHING
        """
        )

        conn.commit()
        print("✔ データベース初期化完了", file=os.sys.stderr)
    except Error as e:
        conn.rollback()
        raise RuntimeError(f"データベース初期化エラー: {e}") from e
    finally:
        cur.close()


# ----------------------------------------------------------------------
# 3. リトライ付きクエリ実行
# ----------------------------------------------------------------------
def execute_with_retry(conn, query, params=None, *, max_retries=3):
    """
    クエリ実行をリトライ付きで行う
    - SELECT 系のみ想定（fetchall() で返す）
    - 失敗時はロールバックする
    """
    for attempt in range(1, max_retries + 1):
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cur.execute(query, params)
            rows = cur.fetchall()
            cur.close()
            return rows
        except Error as e:
            cur.close()
            conn.rollback()
            if attempt < max_retries:
                print(f"⚠ クエリ失敗 (retry {attempt}/{max_retries}): {e}", file=os.sys.stderr)
            else:
                raise


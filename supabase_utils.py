#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase ユーティリティモジュール

・CLI オプション / 環境変数を吸収して接続情報を生成
・Supabaseクライアント接続管理
・初期化（テーブル＋基本データ投入）も提供
"""

import os
from dotenv import load_dotenv
from supabase import create_client, Client

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
        "url": _env("SUPABASE_URL"),
        "key": _env("SUPABASE_KEY"),
        # PostgreSQL直接接続用（オプション）
        "host": _env("PG_HOST"),
        "port": int(_env("PG_PORT", 5432)),
        "database": _env("PG_DATABASE", "postgres"),
        "user": _env("PG_USER", "postgres"),
        "password": _env("PG_PASSWORD"),
    }


def add_common_args(parser):
    """
    argparse.ArgumentParser に共通 Supabase オプションを追加

    --url         : Supabase プロジェクト URL
    --key         : Supabase API キー
    -H / --host   : PostgreSQL 直接接続用ホスト名
    -P / --port   : PostgreSQL 直接接続用ポート番号
    -d / --database: PostgreSQL 直接接続用データベース名
    -U / --user   : PostgreSQL 直接接続用ユーザー
    -W / --password: PostgreSQL 直接接続用パスワード
    --direct      : PostgreSQL 直接接続を使用
    """
    parser.add_argument("--url", help="Supabase プロジェクト URL (SUPABASE_URL)")
    parser.add_argument("--key", help="Supabase API キー (SUPABASE_KEY)")
    parser.add_argument("-H", "--host", help="PostgreSQL ホスト (PG_HOST)")
    parser.add_argument("-P", "--port", type=int, help="PostgreSQL ポート (PG_PORT)")
    parser.add_argument("-d", "--database", help="データベース名 (PG_DATABASE)")
    parser.add_argument("-U", "--user", help="ユーザー名 (PG_USER)")
    parser.add_argument("-W", "--password", help="パスワード (PG_PASSWORD)")
    parser.add_argument("--direct", action="store_true", help="PostgreSQL 直接接続を使用")
    return parser


def _merge_cli_env(args) -> dict:
    """Namespace と環境変数をマージして最終的な接続設定を作成"""
    cfg = get_env_config()

    if getattr(args, "url", None):
        cfg["url"] = args.url
    if getattr(args, "key", None):
        cfg["key"] = args.key
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


def get_supabase_client_from_args(args) -> Client:
    """
    CLI / 環境変数の情報を用いて Supabase クライアントを作成

    --direct オプションが指定された場合は PostgreSQL 直接接続を使用
    """
    cfg = _merge_cli_env(args)

    # 直接接続が指定された場合
    if getattr(args, "direct", False):
        return get_direct_postgres_client(cfg)
    
    # Supabaseクライアント接続
    if not cfg["url"] or not cfg["key"]:
        raise ValueError(
            "Supabase URL または API キーが指定されていません "
            "（--url/--key または SUPABASE_URL/SUPABASE_KEY）"
        )

    try:
        return create_client(cfg["url"], cfg["key"])
    except Exception as e:
        raise RuntimeError(f"Supabase 接続失敗: {e}") from e


def get_direct_postgres_client(cfg):
    """PostgreSQL 直接接続用のクライアント（psycopg2使用）"""
    import psycopg2
    from psycopg2 import extras

    # dbname が無いと psycopg2 が失敗するため明示チェック
    if not cfg["database"]:
        raise ValueError(
            "データベース名が指定されていません（--database または PG_DATABASE）"
        )

    try:
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
        )
        
        # psycopg2接続をSupabaseクライアント風にラップするクラス
        class PostgreSQLWrapper:
            def __init__(self, connection):
                self.conn = connection
            
            def rpc(self, function_name, params=None):
                """RPC風のインターフェース"""
                class RPCResult:
                    def __init__(self, data):
                        self.data = data
                    
                    def execute(self):
                        return self
                
                cur = self.conn.cursor(cursor_factory=extras.DictCursor)
                try:
                    if function_name == 'get_words_with_pos':
                        cur.execute("""
                            SELECT w.reading, w.word, p.name AS pos_name
                            FROM words w
                            JOIN pos_codes p ON p.code = w.pos_code
                            ORDER BY w.reading, w.word
                        """)
                        rows = cur.fetchall()
                        data = [dict(row) for row in rows]
                        return RPCResult(data)
                    else:
                        raise ValueError(f"未対応のRPC関数: {function_name}")
                finally:
                    cur.close()
        
        return PostgreSQLWrapper(conn)
        
    except Exception as e:
        raise RuntimeError(f"PostgreSQL 接続失敗: {e}") from e


def get_supabase_client() -> Client:
    """環境変数ベースのみで接続（スクリプト外から直接使う場合用）"""
    class _Dummy:  # 環境変数だけを使うためのダミー Namespace
        pass

    return get_supabase_client_from_args(_Dummy())


# ----------------------------------------------------------------------
# 2. 初期化処理（RPC関数使用）
# ----------------------------------------------------------------------
def initialize_database(supabase: Client):
    """テーブル作成 & 基本データ挿入（Supabase RPC経由）"""
    try:
        # 初期化用のRPC関数を呼び出し
        response = supabase.rpc('initialize_medical_dict_tables', {}).execute()
        print("✔ データベース初期化完了", file=os.sys.stderr)
        return response
    except Exception as e:
        raise RuntimeError(f"データベース初期化エラー: {e}") from e


# Supabase側で事前に作成が必要なRPC関数
def get_required_rpc_functions():
    """
    Supabase SQL Editor で事前に実行が必要なRPC関数のSQLを返す
    """
    return """
-- 初期化用RPC関数
CREATE OR REPLACE FUNCTION initialize_medical_dict_tables()
RETURNS TEXT AS $$
BEGIN
    -- pos_codes テーブル作成
    CREATE TABLE IF NOT EXISTS pos_codes (
        code SERIAL PRIMARY KEY,
        name VARCHAR(50) UNIQUE NOT NULL
    );

    -- attr_codes テーブル作成  
    CREATE TABLE IF NOT EXISTS attr_codes (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) UNIQUE NOT NULL
    );

    -- words テーブル作成
    CREATE TABLE IF NOT EXISTS words (
        id SERIAL PRIMARY KEY,
        reading VARCHAR(255) NOT NULL,
        word VARCHAR(255) NOT NULL,
        pos_code INTEGER REFERENCES pos_codes(code),
        attr_id INTEGER REFERENCES attr_codes(id),
        collocation TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(reading, word, pos_code)
    );

    -- トリガー関数作成
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $func$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $func$ LANGUAGE plpgsql;

    -- トリガー作成
    DROP TRIGGER IF EXISTS update_words_updated_at ON words;
    CREATE TRIGGER update_words_updated_at
        BEFORE UPDATE ON words
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();

    -- 基本品詞データ挿入
    INSERT INTO pos_codes (name) VALUES ('固有名詞'), ('普通名詞')
    ON CONFLICT (name) DO NOTHING;

    RETURN 'Database initialized successfully';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- データ取得用RPC関数
CREATE OR REPLACE FUNCTION get_words_with_pos()
RETURNS TABLE(reading TEXT, word TEXT, pos_name TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT w.reading::TEXT, w.word::TEXT, p.name::TEXT AS pos_name
    FROM words w
    JOIN pos_codes p ON p.code = w.pos_code
    ORDER BY w.reading, w.word;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 権限付与
GRANT EXECUTE ON FUNCTION initialize_medical_dict_tables() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION get_words_with_pos() TO anon, authenticated, service_role;
"""


# ----------------------------------------------------------------------
# 3. エラーハンドリング付きクエリ実行
# ----------------------------------------------------------------------
def execute_with_retry(supabase: Client, rpc_function, params=None, *, max_retries=3):
    """
    RPC実行をリトライ付きで行う
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = supabase.rpc(rpc_function, params or {}).execute()
            return response.data
        except Exception as e:
            if attempt < max_retries:
                print(f"⚠ RPC失敗 (retry {attempt}/{max_retries}): {e}", file=os.sys.stderr)
            else:
                raise


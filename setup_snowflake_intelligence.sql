-- ============================================================
-- Snowflake Intelligence セットアップ
-- 広告バナー分析アドバイザー エージェント
-- ============================================================
-- 前提条件:
--   1. setup_ad_banner_analytics.sql 実行済み (テーブル・データ作成)
--   2. ad_banner_semantic_view.sql 実行済み (セマンティックビュー作成)
--   3. setup_knowledge_base.sql 実行済み (Cortex Search Service作成)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE KFUKAMORI_GEN_DB;
USE SCHEMA AD_BANNER_ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1. Snowflake Intelligence オブジェクトの初期化
--    (アカウントに1つのみ。既に存在すればスキップ)
-- ============================================================
CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

-- ============================================================
-- 2. Cortex Agent オブジェクトの作成
--    ツール: Cortex Analyst, Cortex Search, Web Search, Chart
-- ============================================================
CREATE OR REPLACE AGENT KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.AD_BANNER_ADVISOR_AGENT
  COMMENT = '広告バナー分析AIエージェント - Cortex Analyst + Cortex Search + Web Search + Chart'
  PROFILE = '{"display_name": "広告バナー分析アドバイザー", "color": "blue"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto

  orchestration:
    budget:
      seconds: 120
      tokens: 100000

  instructions:
    response: |
      日本語で回答してください。
      データに基づいた具体的な数値を含む分析・改善提案を行ってください。
      過去ナレッジやWeb検索の結果を引用する場合は出典を明記してください。
      データ分析結果がある場合は、積極的にチャートを生成して視覚的にわかりやすく回答してください。
    orchestration: |
      あなたは広告バナー最適化の専門AIアドバイザーです。
      ユーザーの質問に応じて、以下のツールを適切に使い分けてください：
      - ad_performance_analyst: キャンペーン配信データ（CTR, CVR, CPA, インプレッション、クリック、コンバージョン等）の分析にはこのツールを使う
      - knowledge_search: 過去の社内分析ナレッジやベストプラクティスを検索するにはこのツールを使う
      - web_search: 最新の広告業界トレンドやデジタルマーケティングの一般知識についてはこのツールを使う
      - data_to_chart: データ分析結果を視覚化する場合はこのツールを使う
      複数のツールを組み合わせて、データ＋ナレッジ＋最新情報を統合した回答を心がけてください。
    sample_questions:
      - question: "現在のキャンペーンでCTRが最も高いバナーはどれですか？"
      - question: "過去にCTAボタンの色を変更して成功した事例はありますか？"
      - question: "最新のディスプレイ広告のCTRベンチマークを教えてください"
      - question: "キャンペーン別のCVR推移をグラフで見せてください"
      - question: "モバイルとデスクトップでCPAに差はありますか？"

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "ad_performance_analyst"
        description: "広告バナーの配信パフォーマンスデータ（キャンペーン、バナー、インプレッション、クリック、コンバージョン、CTR、CVR、CPA、デバイス別・地域別データ）をSQLで分析します"
    - tool_spec:
        type: "cortex_search"
        name: "knowledge_search"
        description: "過去の社内広告分析レポートやナレッジベースから、CTA最適化、クリエイティブ改善、A/Bテスト結果、業界別ベストプラクティスなどの知見を検索します"
    - tool_spec:
        type: "web_search"
        name: "web_search"
    - tool_spec:
        type: "data_to_chart"
        name: "data_to_chart"
        description: "データ分析結果からチャートやグラフを自動生成して視覚的にわかりやすく表示します"

  tool_resources:
    ad_performance_analyst:
      semantic_view: "KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.AD_PERFORMANCE_SEMANTIC_VIEW"
      execution_environment:
        type: "warehouse"
        warehouse: "COMPUTE_WH"
        query_timeout: 60
    knowledge_search:
      search_service: "KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.KNOWLEDGE_SEARCH_SERVICE"
      max_results: 5
  $$;

-- ============================================================
-- 3. 権限設定
-- ============================================================
-- Cortex AI 機能利用権限
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ACCOUNTADMIN;

-- エージェント利用権限
GRANT USAGE ON AGENT KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.AD_BANNER_ADVISOR_AGENT TO ROLE ACCOUNTADMIN;

-- ============================================================
-- 4. Snowflake Intelligence にエージェントを登録
-- ============================================================
ALTER SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT
  ADD AGENT KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.AD_BANNER_ADVISOR_AGENT;

-- ============================================================
-- 確認クエリ
-- ============================================================
-- エージェント設定確認
DESCRIBE AGENT KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.AD_BANNER_ADVISOR_AGENT;

-- Snowflake Intelligence 登録確認
SHOW AGENTS IN SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;

-- ============================================================
-- 利用方法:
--   Snowsight にログイン → 左メニュー「AI & ML」→「Agents」
--   → 「広告バナー分析アドバイザー」を選択してチャット開始
--
-- サンプル質問:
--   ・現在のキャンペーンでCTRが最も高いバナーはどれですか？
--   ・過去にCTAボタンの色を変更して成功した事例はありますか？
--   ・最新のディスプレイ広告のCTRベンチマークを教えてください
--   ・キャンペーン別のCVR推移をグラフで見せてください
--   ・モバイルとデスクトップでCPAに差はありますか？
-- ============================================================

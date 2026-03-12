-- ============================================================================
-- ナレッジベース セットアップSQL
-- PDF格納ステージ、チャンクテーブル、Cortex Search Service作成
-- ============================================================================

USE SCHEMA KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS;

-- ============================================================================
-- ステージ作成 (ナレッジPDF格納用)
-- ============================================================================
CREATE STAGE IF NOT EXISTS KNOWLEDGE_DOCS
  DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- ============================================================================
-- チャンクテーブル作成
-- ============================================================================
CREATE OR REPLACE TABLE KNOWLEDGE_CHUNKS (
    CHUNK_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    DOC_FILENAME    VARCHAR(500) NOT NULL,
    DOC_TITLE       VARCHAR(500),
    CHUNK_INDEX     NUMBER NOT NULL,
    CHUNK_TEXT      VARCHAR(8000) NOT NULL,
    CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- Cortex Search Service作成
-- (チャンクテーブルにデータ投入後に実行)
-- ============================================================================
-- CREATE OR REPLACE CORTEX SEARCH SERVICE KNOWLEDGE_SEARCH_SERVICE
--   ON CHUNK_TEXT
--   ATTRIBUTES DOC_TITLE, DOC_FILENAME
--   WAREHOUSE = COMPUTE_WH
--   TARGET_LAG = '1 hour'
--   AS (
--     SELECT
--       CHUNK_TEXT,
--       DOC_TITLE,
--       DOC_FILENAME,
--       CHUNK_INDEX
--     FROM KNOWLEDGE_CHUNKS
--   );

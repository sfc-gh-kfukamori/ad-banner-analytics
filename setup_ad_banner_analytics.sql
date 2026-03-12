-- ============================================================================
-- 広告バナー分析 セットアップSQL
-- テーブル作成 + ダミーデータ投入 + ステージ作成
-- ============================================================================

USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS KFUKAMORI_GEN_DB;
CREATE SCHEMA IF NOT EXISTS KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS;
USE SCHEMA KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS;

-- ============================================================================
-- ステージ作成 (バナー画像格納用)
-- ============================================================================
CREATE STAGE IF NOT EXISTS BANNER_IMAGES
  DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- ============================================================================
-- テーブル作成
-- ============================================================================

-- キャンペーンマスタ
CREATE OR REPLACE TABLE CAMPAIGNS (
    CAMPAIGN_ID       NUMBER AUTOINCREMENT PRIMARY KEY,
    CAMPAIGN_NAME     VARCHAR(200) NOT NULL,
    ADVERTISER        VARCHAR(200) NOT NULL,
    INDUSTRY          VARCHAR(100) NOT NULL,
    OBJECTIVE         VARCHAR(50)  NOT NULL,  -- AWARENESS / CONSIDERATION / CONVERSION
    TARGET_AUDIENCE   VARCHAR(200),
    BUDGET_JPY        NUMBER(15,0),
    START_DATE        DATE NOT NULL,
    END_DATE          DATE NOT NULL,
    STATUS            VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE / PAUSED / COMPLETED
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 広告バナーマスタ
CREATE OR REPLACE TABLE AD_BANNERS (
    BANNER_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    CAMPAIGN_ID       NUMBER NOT NULL REFERENCES CAMPAIGNS(CAMPAIGN_ID),
    BANNER_NAME       VARCHAR(200) NOT NULL,
    BANNER_SIZE       VARCHAR(20)  NOT NULL,  -- 300x250, 728x90, 160x600, 320x50, 336x280
    IMAGE_PATH        VARCHAR(500),           -- ステージ内の画像パス
    CREATIVE_TYPE     VARCHAR(50)  NOT NULL,  -- STATIC / ANIMATED / VIDEO
    PRIMARY_COLOR     VARCHAR(30),
    HEADLINE_TEXT     VARCHAR(500),
    CTA_TEXT          VARCHAR(100),
    CTA_COLOR         VARCHAR(30),
    DESIGN_STYLE      VARCHAR(50),            -- MINIMAL / BOLD / PHOTOGRAPHIC / ILLUSTRATIVE
    APPEAL_TYPE       VARCHAR(50),            -- PRICE / BRAND / FEATURE / EMOTIONAL
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- A/Bテスト定義
CREATE OR REPLACE TABLE AB_TESTS (
    TEST_ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    TEST_NAME         VARCHAR(200) NOT NULL,
    CAMPAIGN_ID       NUMBER NOT NULL REFERENCES CAMPAIGNS(CAMPAIGN_ID),
    BANNER_A_ID       NUMBER NOT NULL REFERENCES AD_BANNERS(BANNER_ID),
    BANNER_B_ID       NUMBER NOT NULL REFERENCES AD_BANNERS(BANNER_ID),
    TEST_VARIABLE     VARCHAR(100) NOT NULL,  -- IMAGE / CTA / COLOR / LAYOUT / HEADLINE
    TRAFFIC_SPLIT     NUMBER(5,2) DEFAULT 50.00,  -- バナーAへの配分%
    START_DATE        DATE NOT NULL,
    END_DATE          DATE,
    STATUS            VARCHAR(20) DEFAULT 'RUNNING',  -- RUNNING / COMPLETED / STOPPED
    WINNER_BANNER_ID  NUMBER,
    CONFIDENCE_LEVEL  NUMBER(5,2),
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 日別配信結果
CREATE OR REPLACE TABLE DELIVERY_RESULTS (
    RESULT_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    BANNER_ID         NUMBER NOT NULL REFERENCES AD_BANNERS(BANNER_ID),
    DELIVERY_DATE     DATE NOT NULL,
    DEVICE_TYPE       VARCHAR(20) NOT NULL,  -- MOBILE / DESKTOP / TABLET
    REGION            VARCHAR(50) NOT NULL,
    IMPRESSIONS       NUMBER(15,0) DEFAULT 0,
    CLICKS            NUMBER(15,0) DEFAULT 0,
    CONVERSIONS       NUMBER(15,0) DEFAULT 0,
    COST_JPY          NUMBER(15,2) DEFAULT 0,
    VIEWABLE_IMPS     NUMBER(15,0) DEFAULT 0,
    VIDEO_VIEWS       NUMBER(15,0) DEFAULT 0,
    ENGAGEMENT_TIME_SEC NUMBER(10,2) DEFAULT 0
);

-- A/Bテスト結果サマリー
CREATE OR REPLACE TABLE AB_TEST_RESULTS (
    RESULT_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    TEST_ID           NUMBER NOT NULL REFERENCES AB_TESTS(TEST_ID),
    BANNER_ID         NUMBER NOT NULL REFERENCES AD_BANNERS(BANNER_ID),
    TOTAL_IMPRESSIONS NUMBER(15,0),
    TOTAL_CLICKS      NUMBER(15,0),
    TOTAL_CONVERSIONS NUMBER(15,0),
    CTR               NUMBER(10,6),
    CVR               NUMBER(10,6),
    CPA_JPY           NUMBER(15,2),
    TOTAL_COST_JPY    NUMBER(15,2),
    P_VALUE           NUMBER(10,8),
    IS_WINNER         BOOLEAN DEFAULT FALSE,
    STATISTICAL_SIGNIFICANCE VARCHAR(20)  -- SIGNIFICANT / NOT_SIGNIFICANT / INCONCLUSIVE
);

-- AI分析結果キャッシュ
CREATE OR REPLACE TABLE BANNER_AI_ANALYSIS (
    ANALYSIS_ID       NUMBER AUTOINCREMENT PRIMARY KEY,
    BANNER_ID         NUMBER NOT NULL REFERENCES AD_BANNERS(BANNER_ID),
    ANALYSIS_TYPE     VARCHAR(50) NOT NULL,  -- CREATIVE_REVIEW / CLASSIFY / SIMILARITY / IMPROVEMENT
    MODEL_USED        VARCHAR(100),
    ANALYSIS_RESULT   VARIANT,
    ANALYZED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ============================================================================
-- ダミーデータ投入
-- ============================================================================

-- キャンペーンデータ
INSERT INTO CAMPAIGNS (CAMPAIGN_NAME, ADVERTISER, INDUSTRY, OBJECTIVE, TARGET_AUDIENCE, BUDGET_JPY, START_DATE, END_DATE, STATUS)
VALUES
    ('春の新生活キャンペーン2025', '家電メーカーA', '家電・エレクトロニクス', 'CONVERSION', '25-34歳 新生活準備層', 5000000, '2025-03-01', '2025-05-31', 'COMPLETED'),
    ('サマーセール プロモーション', 'アパレルブランドB', 'ファッション・アパレル', 'AWARENESS', '18-29歳 女性 ファッション関心層', 3000000, '2025-06-01', '2025-08-31', 'COMPLETED'),
    ('新商品ローンチ - スマートウォッチ', 'テックカンパニーC', 'IT・テクノロジー', 'CONSIDERATION', '30-44歳 テクノロジー関心層', 8000000, '2025-07-15', '2025-10-14', 'COMPLETED'),
    ('年末ギフトキャンペーン', '食品メーカーD', '食品・飲料', 'CONVERSION', '30-49歳 ギフト購入層', 4000000, '2025-11-01', '2025-12-31', 'ACTIVE'),
    ('ブランドリニューアル認知', '化粧品ブランドE', '美容・化粧品', 'AWARENESS', '20-39歳 女性 美容関心層', 6000000, '2025-09-01', '2025-12-31', 'ACTIVE');

-- 広告バナーデータ (各キャンペーン3-4バナー、計18バナー)
INSERT INTO AD_BANNERS (CAMPAIGN_ID, BANNER_NAME, BANNER_SIZE, IMAGE_PATH, CREATIVE_TYPE, PRIMARY_COLOR, HEADLINE_TEXT, CTA_TEXT, CTA_COLOR, DESIGN_STYLE, APPEAL_TYPE)
VALUES
    -- キャンペーン1: 春の新生活
    (1, '新生活_写真メイン_赤CTA',      '300x250', 'campaign1/banner_01.png', 'STATIC', '#FFFFFF', '新生活、始めよう。最大30%OFF', '今すぐチェック', '#E53935', 'PHOTOGRAPHIC', 'PRICE'),
    (1, '新生活_ミニマル_青CTA',        '300x250', 'campaign1/banner_02.png', 'STATIC', '#F5F5F5', 'シンプルに、新しく。春の家電フェア', '詳しく見る', '#1E88E5', 'MINIMAL', 'BRAND'),
    (1, '新生活_ボールド_黄CTA',        '728x90',  'campaign1/banner_03.png', 'STATIC', '#212121', '今だけ！春の超特価セール', '特価を見る', '#FDD835', 'BOLD', 'PRICE'),
    (1, '新生活_イラスト_緑CTA',        '320x50',  'campaign1/banner_04.png', 'STATIC', '#E8F5E9', 'ワクワクする新生活を応援', 'キャンペーン詳細', '#43A047', 'ILLUSTRATIVE', 'EMOTIONAL'),

    -- キャンペーン2: サマーセール
    (2, 'サマセ_ビビッド_ピンクCTA',    '300x250', 'campaign2/banner_05.png', 'STATIC', '#FCE4EC', 'SUMMER SALE 最大50%OFF', 'SHOP NOW', '#E91E63', 'BOLD', 'PRICE'),
    (2, 'サマセ_写真_白CTA',           '300x250', 'campaign2/banner_06.png', 'STATIC', '#FFFFFF', 'この夏のトレンドをお得に', 'コレクションを見る', '#FFFFFF', 'PHOTOGRAPHIC', 'BRAND'),
    (2, 'サマセ_アニメ_オレンジCTA',    '336x280', 'campaign2/banner_07.png', 'ANIMATED', '#FFF3E0', '期間限定サマーコレクション', '今すぐ購入', '#FF6F00', 'BOLD', 'PRICE'),

    -- キャンペーン3: スマートウォッチ新商品
    (3, 'SW_プロダクト_黒背景',         '300x250', 'campaign3/banner_08.png', 'STATIC', '#000000', 'The Next Smart. 新次元のスマートウォッチ', '製品を見る', '#00BCD4', 'MINIMAL', 'FEATURE'),
    (3, 'SW_ライフスタイル_白背景',     '300x250', 'campaign3/banner_09.png', 'STATIC', '#FAFAFA', 'あなたの健康を、腕の上に。', '詳しく見る', '#4CAF50', 'PHOTOGRAPHIC', 'EMOTIONAL'),
    (3, 'SW_スペック訴求_ダーク',       '728x90',  'campaign3/banner_10.png', 'STATIC', '#1A1A2E', 'バッテリー7日間 | 血中酸素 | GPS', '今すぐ予約', '#FF5722', 'MINIMAL', 'FEATURE'),
    (3, 'SW_動画プレビュー',            '300x250', 'campaign3/banner_11.png', 'VIDEO', '#0D0D0D', 'Watch the Difference', '動画を見る', '#2196F3', 'PHOTOGRAPHIC', 'BRAND'),

    -- キャンペーン4: 年末ギフト
    (4, 'ギフト_暖色系_感謝',           '300x250', 'campaign4/banner_12.png', 'STATIC', '#FBE9E7', '大切な人に、おいしい贈り物を', 'ギフトを探す', '#BF360C', 'PHOTOGRAPHIC', 'EMOTIONAL'),
    (4, 'ギフト_高級感_金CTA',          '300x250', 'campaign4/banner_13.png', 'STATIC', '#3E2723', 'プレミアムギフトコレクション', 'コレクションを見る', '#FFD700', 'MINIMAL', 'BRAND'),
    (4, 'ギフト_カジュアル_赤CTA',      '336x280', 'campaign4/banner_14.png', 'STATIC', '#FFEBEE', '冬のご褒美ギフト特集', '今すぐ注文', '#D32F2F', 'ILLUSTRATIVE', 'PRICE'),

    -- キャンペーン5: 化粧品ブランドリニューアル
    (5, 'コスメ_エレガント_ローズ',     '300x250', 'campaign5/banner_15.png', 'STATIC', '#F8F0F0', 'New Me, New Beauty.', 'ブランドを体験', '#C2185B', 'MINIMAL', 'BRAND'),
    (5, 'コスメ_ナチュラル_グリーン',   '300x250', 'campaign5/banner_16.png', 'STATIC', '#E8F5E9', '自然由来成分93% 新スキンケアライン', '成分を見る', '#2E7D32', 'PHOTOGRAPHIC', 'FEATURE'),
    (5, 'コスメ_ビフォーアフター',      '728x90',  'campaign5/banner_17.png', 'STATIC', '#FFFFFF', '14日間で実感。透明感のある素肌へ', '無料サンプル', '#9C27B0', 'PHOTOGRAPHIC', 'FEATURE'),
    (5, 'コスメ_インフルエンサー',      '300x250', 'campaign5/banner_18.png', 'STATIC', '#FFF8E1', '人気美容家も愛用！リニューアルコスメ', '詳しく見る', '#F57C00', 'PHOTOGRAPHIC', 'EMOTIONAL');

-- A/Bテスト定義
INSERT INTO AB_TESTS (TEST_NAME, CAMPAIGN_ID, BANNER_A_ID, BANNER_B_ID, TEST_VARIABLE, TRAFFIC_SPLIT, START_DATE, END_DATE, STATUS, WINNER_BANNER_ID, CONFIDENCE_LEVEL)
VALUES
    ('新生活_写真vsミニマル',    1, 1, 2, 'DESIGN_STYLE', 50.00, '2025-03-01', '2025-03-31', 'COMPLETED', 1, 96.5),
    ('新生活_CTA色比較',         1, 1, 3, 'CTA_COLOR',    50.00, '2025-04-01', '2025-04-30', 'COMPLETED', 3, 92.3),
    ('サマセ_静止画vsアニメ',    2, 5, 7, 'CREATIVE_TYPE', 50.00, '2025-06-15', '2025-07-15', 'COMPLETED', 7, 98.1),
    ('SW_機能訴求vs感情訴求',    3, 8, 9, 'APPEAL_TYPE',  50.00, '2025-08-01', '2025-08-31', 'COMPLETED', 9, 88.7),
    ('コスメ_ブランドvs機能',    5, 15, 16, 'APPEAL_TYPE', 50.00, '2025-10-01', '2025-10-31', 'RUNNING', NULL, NULL);


-- ============================================================================
-- 日別配信結果データ生成 (プロシージャ)
-- ============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_DELIVERY_DATA()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_banner_id NUMBER;
    v_campaign_id NUMBER;
    v_start_date DATE;
    v_end_date DATE;
    v_current_date DATE;
    v_device VARCHAR;
    v_region VARCHAR;
    v_base_imp NUMBER;
    v_imp NUMBER;
    v_clicks NUMBER;
    v_conversions NUMBER;
    v_cost NUMBER;
    v_ctr_base FLOAT;
    v_cvr_base FLOAT;
    v_cpc_base FLOAT;
    v_row_count NUMBER DEFAULT 0;

    -- バナーごとのカーソル
    c_banners CURSOR FOR
        SELECT b.BANNER_ID, b.CAMPAIGN_ID, c.START_DATE, c.END_DATE
        FROM AD_BANNERS b
        JOIN CAMPAIGNS c ON b.CAMPAIGN_ID = c.CAMPAIGN_ID;
BEGIN
    -- 既存データクリア
    DELETE FROM DELIVERY_RESULTS;

    FOR rec IN c_banners DO
        v_banner_id := rec.BANNER_ID;
        v_campaign_id := rec.CAMPAIGN_ID;
        v_start_date := rec.START_DATE;
        v_end_date := LEAST(rec.END_DATE, '2025-12-15'::DATE);

        -- バナーごとにベースパフォーマンスを設定 (ランダム性あり)
        v_base_imp := 800 + MOD(v_banner_id * 137, 1200);
        v_ctr_base := 0.005 + (MOD(v_banner_id * 73, 30)) / 1000.0;   -- 0.5% ~ 3.5%
        v_cvr_base := 0.01 + (MOD(v_banner_id * 41, 40)) / 1000.0;    -- 1.0% ~ 5.0%
        v_cpc_base := 30 + MOD(v_banner_id * 59, 70);                  -- 30 ~ 100円

        v_current_date := v_start_date;

        WHILE (v_current_date <= v_end_date) DO
            -- デバイスタイプごとにループ
            FOR v_device IN ('MOBILE', 'DESKTOP', 'TABLET') DO
                -- 地域ごとにループ
                FOR v_region IN ('関東', '関西', '中部', '九州', '北海道・東北') DO

                    -- デバイス別の配分比率
                    LET device_factor FLOAT := CASE
                        WHEN v_device = 'MOBILE'  THEN 0.55
                        WHEN v_device = 'DESKTOP' THEN 0.35
                        ELSE 0.10
                    END;

                    -- 地域別の配分比率
                    LET region_factor FLOAT := CASE
                        WHEN v_region = '関東'         THEN 0.40
                        WHEN v_region = '関西'         THEN 0.25
                        WHEN v_region = '中部'         THEN 0.15
                        WHEN v_region = '九州'         THEN 0.12
                        ELSE 0.08
                    END;

                    -- 曜日効果 (土日はimp増)
                    LET dow_factor FLOAT := CASE DAYOFWEEK(v_current_date)
                        WHEN 0 THEN 1.15  -- 日
                        WHEN 6 THEN 1.20  -- 土
                        ELSE 1.0
                    END;

                    -- ランダム変動 (RANDOM()で日付ごとに異なるシードを生成)
                    LET rand_factor FLOAT := 0.8 + (MOD(ABS(HASH(v_banner_id || v_current_date::VARCHAR || v_device || v_region)), 400)) / 1000.0;

                    -- インプレッション計算
                    v_imp := GREATEST(1, ROUND(v_base_imp * device_factor * region_factor * dow_factor * rand_factor));

                    -- CTRにもランダム変動
                    LET ctr_rand FLOAT := v_ctr_base * (0.7 + (MOD(ABS(HASH(v_banner_id * 3 || v_current_date::VARCHAR || v_device)), 600)) / 1000.0);
                    v_clicks := GREATEST(0, ROUND(v_imp * ctr_rand));

                    -- CVRにもランダム変動
                    LET cvr_rand FLOAT := v_cvr_base * (0.6 + (MOD(ABS(HASH(v_banner_id * 7 || v_current_date::VARCHAR || v_region)), 800)) / 1000.0);
                    v_conversions := GREATEST(0, ROUND(v_clicks * cvr_rand));

                    -- コスト計算 (CPC課金ベース)
                    v_cost := ROUND(v_clicks * v_cpc_base * (0.9 + (MOD(ABS(HASH(v_banner_id * 11 || v_current_date::VARCHAR)), 200)) / 1000.0), 2);

                    INSERT INTO DELIVERY_RESULTS (
                        BANNER_ID, DELIVERY_DATE, DEVICE_TYPE, REGION,
                        IMPRESSIONS, CLICKS, CONVERSIONS, COST_JPY,
                        VIEWABLE_IMPS, ENGAGEMENT_TIME_SEC
                    )
                    VALUES (
                        v_banner_id, v_current_date, v_device, v_region,
                        v_imp, v_clicks, v_conversions, v_cost,
                        ROUND(v_imp * 0.65), ROUND(v_clicks * 12.5, 2)
                    );

                    v_row_count := v_row_count + 1;

                END FOR;
            END FOR;

            v_current_date := DATEADD(DAY, 1, v_current_date);
        END WHILE;
    END FOR;

    RETURN 'Generated ' || v_row_count::VARCHAR || ' delivery result rows';
END;
$$;

-- プロシージャ実行
CALL GENERATE_DELIVERY_DATA();

-- ============================================================================
-- A/Bテスト結果サマリーを配信データから集計
-- ============================================================================
INSERT INTO AB_TEST_RESULTS (TEST_ID, BANNER_ID, TOTAL_IMPRESSIONS, TOTAL_CLICKS, TOTAL_CONVERSIONS, CTR, CVR, CPA_JPY, TOTAL_COST_JPY, P_VALUE, IS_WINNER, STATISTICAL_SIGNIFICANCE)
WITH test_metrics AS (
    SELECT
        t.TEST_ID,
        dr.BANNER_ID,
        SUM(dr.IMPRESSIONS) AS TOTAL_IMPRESSIONS,
        SUM(dr.CLICKS) AS TOTAL_CLICKS,
        SUM(dr.CONVERSIONS) AS TOTAL_CONVERSIONS,
        CASE WHEN SUM(dr.IMPRESSIONS) > 0 THEN SUM(dr.CLICKS)::FLOAT / SUM(dr.IMPRESSIONS) ELSE 0 END AS CTR,
        CASE WHEN SUM(dr.CLICKS) > 0 THEN SUM(dr.CONVERSIONS)::FLOAT / SUM(dr.CLICKS) ELSE 0 END AS CVR,
        CASE WHEN SUM(dr.CONVERSIONS) > 0 THEN SUM(dr.COST_JPY) / SUM(dr.CONVERSIONS) ELSE 0 END AS CPA_JPY,
        SUM(dr.COST_JPY) AS TOTAL_COST_JPY
    FROM AB_TESTS t
    JOIN DELIVERY_RESULTS dr
        ON dr.BANNER_ID IN (t.BANNER_A_ID, t.BANNER_B_ID)
        AND dr.DELIVERY_DATE BETWEEN t.START_DATE AND COALESCE(t.END_DATE, CURRENT_DATE())
    GROUP BY t.TEST_ID, dr.BANNER_ID
)
SELECT
    tm.TEST_ID,
    tm.BANNER_ID,
    tm.TOTAL_IMPRESSIONS,
    tm.TOTAL_CLICKS,
    tm.TOTAL_CONVERSIONS,
    tm.CTR,
    tm.CVR,
    tm.CPA_JPY,
    tm.TOTAL_COST_JPY,
    -- p値は擬似的に計算 (実際の統計計算を簡略化)
    CASE
        WHEN t.CONFIDENCE_LEVEL IS NOT NULL THEN (100 - t.CONFIDENCE_LEVEL) / 100.0
        ELSE 0.15
    END AS P_VALUE,
    CASE WHEN tm.BANNER_ID = t.WINNER_BANNER_ID THEN TRUE ELSE FALSE END AS IS_WINNER,
    CASE
        WHEN t.CONFIDENCE_LEVEL >= 95 THEN 'SIGNIFICANT'
        WHEN t.CONFIDENCE_LEVEL >= 90 THEN 'MARGINALLY_SIGNIFICANT'
        WHEN t.CONFIDENCE_LEVEL IS NULL THEN 'INCONCLUSIVE'
        ELSE 'NOT_SIGNIFICANT'
    END AS STATISTICAL_SIGNIFICANCE
FROM test_metrics tm
JOIN AB_TESTS t ON tm.TEST_ID = t.TEST_ID;


-- ============================================================================
-- 便利ビュー作成
-- ============================================================================

-- バナー配信サマリービュー
CREATE OR REPLACE VIEW V_BANNER_PERFORMANCE AS
SELECT
    c.CAMPAIGN_ID,
    c.CAMPAIGN_NAME,
    c.ADVERTISER,
    c.INDUSTRY,
    c.OBJECTIVE,
    b.BANNER_ID,
    b.BANNER_NAME,
    b.BANNER_SIZE,
    b.CREATIVE_TYPE,
    b.DESIGN_STYLE,
    b.APPEAL_TYPE,
    b.PRIMARY_COLOR,
    b.HEADLINE_TEXT,
    b.CTA_TEXT,
    b.CTA_COLOR,
    b.IMAGE_PATH,
    dr.DELIVERY_DATE,
    dr.DEVICE_TYPE,
    dr.REGION,
    dr.IMPRESSIONS,
    dr.CLICKS,
    dr.CONVERSIONS,
    dr.COST_JPY,
    dr.VIEWABLE_IMPS,
    dr.ENGAGEMENT_TIME_SEC,
    CASE WHEN dr.IMPRESSIONS > 0 THEN dr.CLICKS::FLOAT / dr.IMPRESSIONS ELSE 0 END AS CTR,
    CASE WHEN dr.CLICKS > 0 THEN dr.CONVERSIONS::FLOAT / dr.CLICKS ELSE 0 END AS CVR,
    CASE WHEN dr.CONVERSIONS > 0 THEN dr.COST_JPY / dr.CONVERSIONS ELSE NULL END AS CPA_JPY,
    CASE WHEN dr.IMPRESSIONS > 0 THEN dr.VIEWABLE_IMPS::FLOAT / dr.IMPRESSIONS ELSE 0 END AS VIEWABILITY_RATE
FROM DELIVERY_RESULTS dr
JOIN AD_BANNERS b ON dr.BANNER_ID = b.BANNER_ID
JOIN CAMPAIGNS c ON b.CAMPAIGN_ID = c.CAMPAIGN_ID;

-- キャンペーンサマリービュー
CREATE OR REPLACE VIEW V_CAMPAIGN_SUMMARY AS
SELECT
    c.CAMPAIGN_ID,
    c.CAMPAIGN_NAME,
    c.ADVERTISER,
    c.INDUSTRY,
    c.OBJECTIVE,
    c.BUDGET_JPY,
    c.STATUS,
    COUNT(DISTINCT b.BANNER_ID) AS BANNER_COUNT,
    SUM(dr.IMPRESSIONS) AS TOTAL_IMPRESSIONS,
    SUM(dr.CLICKS) AS TOTAL_CLICKS,
    SUM(dr.CONVERSIONS) AS TOTAL_CONVERSIONS,
    SUM(dr.COST_JPY) AS TOTAL_COST_JPY,
    CASE WHEN SUM(dr.IMPRESSIONS) > 0 THEN SUM(dr.CLICKS)::FLOAT / SUM(dr.IMPRESSIONS) ELSE 0 END AS AVG_CTR,
    CASE WHEN SUM(dr.CLICKS) > 0 THEN SUM(dr.CONVERSIONS)::FLOAT / SUM(dr.CLICKS) ELSE 0 END AS AVG_CVR,
    CASE WHEN SUM(dr.CONVERSIONS) > 0 THEN SUM(dr.COST_JPY) / SUM(dr.CONVERSIONS) ELSE NULL END AS AVG_CPA_JPY,
    CASE WHEN c.BUDGET_JPY > 0 THEN SUM(dr.COST_JPY) / c.BUDGET_JPY * 100 ELSE 0 END AS BUDGET_UTILIZATION_PCT
FROM CAMPAIGNS c
LEFT JOIN AD_BANNERS b ON c.CAMPAIGN_ID = b.CAMPAIGN_ID
LEFT JOIN DELIVERY_RESULTS dr ON b.BANNER_ID = dr.BANNER_ID
GROUP BY c.CAMPAIGN_ID, c.CAMPAIGN_NAME, c.ADVERTISER, c.INDUSTRY, c.OBJECTIVE, c.BUDGET_JPY, c.STATUS;

-- データ確認
SELECT 'CAMPAIGNS' AS TBL, COUNT(*) AS CNT FROM CAMPAIGNS
UNION ALL SELECT 'AD_BANNERS', COUNT(*) FROM AD_BANNERS
UNION ALL SELECT 'AB_TESTS', COUNT(*) FROM AB_TESTS
UNION ALL SELECT 'DELIVERY_RESULTS', COUNT(*) FROM DELIVERY_RESULTS
UNION ALL SELECT 'AB_TEST_RESULTS', COUNT(*) FROM AB_TEST_RESULTS;

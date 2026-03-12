-- ============================================================================
-- 広告バナー分析 セマンティックビュー定義
-- Cortex Analyst による自然言語クエリ用
-- ============================================================================

USE SCHEMA KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS;

CREATE OR REPLACE SEMANTIC VIEW AD_PERFORMANCE_SEMANTIC_VIEW

  TABLES (
    campaigns AS KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.CAMPAIGNS
      PRIMARY KEY (CAMPAIGN_ID)
      COMMENT = 'キャンペーンマスタ。広告キャンペーンの基本情報（広告主、業種、目的、予算、期間）を管理する。',
    banners AS KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.AD_BANNERS
      PRIMARY KEY (BANNER_ID)
      COMMENT = '広告バナーマスタ。バナーのクリエイティブ属性（サイズ、デザインスタイル、訴求タイプ、CTA）を管理する。',
    delivery AS KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.DELIVERY_RESULTS
      PRIMARY KEY (RESULT_ID)
      COMMENT = '日別配信結果。バナーごとの日次配信パフォーマンス（インプレッション、クリック、コンバージョン、費用）をデバイス・地域別に記録する。',
    ab_tests AS KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.AB_TESTS
      PRIMARY KEY (TEST_ID)
      COMMENT = 'A/Bテスト定義。2つのバナー間のテスト設定（テスト変数、トラフィック配分、勝者）を管理する。',
    ab_results AS KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS.AB_TEST_RESULTS
      PRIMARY KEY (RESULT_ID)
      COMMENT = 'A/Bテスト結果サマリー。テストごとの各バナーの集計パフォーマンスと統計的有意性を記録する。'
  )

  RELATIONSHIPS (
    banners_to_campaigns AS banners(CAMPAIGN_ID) REFERENCES campaigns,
    delivery_to_banners AS delivery(BANNER_ID) REFERENCES banners,
    ab_tests_to_campaigns AS ab_tests(CAMPAIGN_ID) REFERENCES campaigns,
    ab_results_to_tests AS ab_results(TEST_ID) REFERENCES ab_tests,
    ab_results_to_banners AS ab_results(BANNER_ID) REFERENCES banners
  )

  -- ============================================================
  -- ディメンション (分析軸)
  -- ============================================================
  DIMENSIONS (
    -- キャンペーン属性
    campaigns.campaign_name_dim AS CAMPAIGN_NAME
      WITH SYNONYMS = ('キャンペーン', 'campaign')
      COMMENT = 'キャンペーン名。例: 春の新生活キャンペーン2025',
    campaigns.advertiser_dim AS ADVERTISER
      WITH SYNONYMS = ('広告主', 'クライアント', 'advertiser')
      COMMENT = '広告主名。例: 家電メーカーA',
    campaigns.industry_dim AS INDUSTRY
      WITH SYNONYMS = ('業種', '業界', 'industry')
      COMMENT = '広告主の業種。例: 家電・エレクトロニクス, ファッション・アパレル',
    campaigns.objective_dim AS OBJECTIVE
      WITH SYNONYMS = ('目的', '目標', 'KPI', 'objective')
      COMMENT = 'キャンペーン目的。AWARENESS=認知, CONSIDERATION=検討, CONVERSION=獲得',
    campaigns.status_dim AS STATUS
      WITH SYNONYMS = ('キャンペーン状態', 'ステータス')
      COMMENT = 'キャンペーンステータス。ACTIVE=配信中, PAUSED=一時停止, COMPLETED=終了',

    -- バナー属性
    banners.banner_name_dim AS BANNER_NAME
      WITH SYNONYMS = ('バナー', 'クリエイティブ', 'banner', 'creative')
      COMMENT = 'バナー名。クリエイティブの識別名',
    banners.banner_size_dim AS BANNER_SIZE
      WITH SYNONYMS = ('サイズ', 'フォーマット', 'size')
      COMMENT = 'バナーサイズ。例: 300x250, 728x90, 320x50, 336x280, 160x600',
    banners.creative_type_dim AS CREATIVE_TYPE
      WITH SYNONYMS = ('タイプ', '種類', 'creative type')
      COMMENT = 'クリエイティブタイプ。STATIC=静止画, ANIMATED=アニメーション, VIDEO=動画',
    banners.design_style_dim AS DESIGN_STYLE
      WITH SYNONYMS = ('デザイン', 'スタイル', 'design')
      COMMENT = 'デザインスタイル。MINIMAL=ミニマル, BOLD=ボールド, PHOTOGRAPHIC=写真系, ILLUSTRATIVE=イラスト系',
    banners.appeal_type_dim AS APPEAL_TYPE
      WITH SYNONYMS = ('訴求', '訴求タイプ', 'appeal')
      COMMENT = '訴求タイプ。PRICE=価格訴求, BRAND=ブランド訴求, FEATURE=機能訴求, EMOTIONAL=感情訴求',
    banners.cta_text_dim AS CTA_TEXT
      WITH SYNONYMS = ('CTA', 'コールトゥアクション', 'ボタンテキスト')
      COMMENT = 'CTAテキスト。例: 今すぐチェック, 詳しく見る, SHOP NOW',
    banners.headline_text_dim AS HEADLINE_TEXT
      WITH SYNONYMS = ('見出し', 'ヘッドライン', 'headline')
      COMMENT = 'バナーの見出しテキスト',

    -- 配信属性
    delivery.delivery_date_dim AS DELIVERY_DATE
      WITH SYNONYMS = ('日付', '配信日', 'date')
      COMMENT = '配信日。日別パフォーマンスの日付',
    delivery.device_type_dim AS DEVICE_TYPE
      WITH SYNONYMS = ('デバイス', '端末', 'device')
      COMMENT = 'デバイスタイプ。MOBILE=モバイル, DESKTOP=デスクトップ, TABLET=タブレット',
    delivery.region_dim AS REGION
      WITH SYNONYMS = ('地域', 'エリア', 'region')
      COMMENT = '配信地域。関東, 関西, 中部, 九州, 北海道・東北',

    -- A/Bテスト属性
    ab_tests.test_name_dim AS TEST_NAME
      WITH SYNONYMS = ('テスト', 'ABテスト', 'test')
      COMMENT = 'A/Bテスト名',
    ab_tests.test_variable_dim AS TEST_VARIABLE
      WITH SYNONYMS = ('テスト変数', 'テスト項目', 'variable')
      COMMENT = 'テスト変数。IMAGE=画像, CTA=CTA, COLOR=色, LAYOUT=レイアウト, HEADLINE=見出し',
    ab_results.significance_dim AS STATISTICAL_SIGNIFICANCE
      WITH SYNONYMS = ('有意性', '有意差', 'significance')
      COMMENT = '統計的有意性。SIGNIFICANT=有意, MARGINALLY_SIGNIFICANT=やや有意, NOT_SIGNIFICANT=有意でない, INCONCLUSIVE=判定不能'
  )

  -- ============================================================
  -- メトリクス (指標)
  -- ============================================================
  METRICS (
    -- 配信量指標
    delivery.impressions_metric AS SUM(IMPRESSIONS)
      WITH SYNONYMS = ('インプレッション', 'imp', '表示回数', 'impressions')
      COMMENT = 'インプレッション数。バナーが表示された回数',
    delivery.clicks_metric AS SUM(CLICKS)
      WITH SYNONYMS = ('クリック', 'click', 'クリック数')
      COMMENT = 'クリック数。バナーがクリックされた回数',
    delivery.conversions_metric AS SUM(CONVERSIONS)
      WITH SYNONYMS = ('コンバージョン', 'CV', '成果', '獲得数', 'conversions')
      COMMENT = 'コンバージョン数。クリック後に成果に至った回数',
    delivery.cost_metric AS SUM(COST_JPY)
      WITH SYNONYMS = ('費用', 'コスト', '広告費', '出稿額', 'cost', 'spend')
      COMMENT = '広告費用（円）。配信に要した費用',
    delivery.viewable_imps_metric AS SUM(VIEWABLE_IMPS)
      WITH SYNONYMS = ('ビューアブル', 'viewable', '視認可能表示')
      COMMENT = 'ビューアブルインプレッション数。視認可能な状態で表示された回数',
    delivery.engagement_time_metric AS SUM(ENGAGEMENT_TIME_SEC)
      WITH SYNONYMS = ('エンゲージメント時間', '滞在時間', 'engagement')
      COMMENT = 'エンゲージメント時間（秒）。ユーザーがバナーに関与した時間',

    -- 予算
    campaigns.budget_metric AS SUM(BUDGET_JPY)
      WITH SYNONYMS = ('予算', 'budget')
      COMMENT = 'キャンペーン予算（円）',

    -- A/Bテスト指標
    ab_results.ab_ctr_metric AS AVG(CTR)
      WITH SYNONYMS = ('テストCTR', 'ABテストクリック率')
      COMMENT = 'A/BテストのCTR（クリック率）',
    ab_results.ab_cvr_metric AS AVG(CVR)
      WITH SYNONYMS = ('テストCVR', 'ABテストコンバージョン率')
      COMMENT = 'A/BテストのCVR（コンバージョン率）',
    ab_results.p_value_metric AS AVG(P_VALUE)
      WITH SYNONYMS = ('p値', 'p-value')
      COMMENT = 'p値。統計的有意性を判定する値。0.05以下で有意',
    ab_tests.confidence_metric AS AVG(CONFIDENCE_LEVEL)
      WITH SYNONYMS = ('信頼水準', '信頼度', 'confidence')
      COMMENT = '信頼水準（%）。A/Bテスト結果の信頼度'
  )

  COMMENT = '広告バナー配信パフォーマンスの分析用セマンティックビュー。キャンペーン、バナー、配信結果を統合し、自然言語での問い合わせを可能にする。';

-- 確認
DESCRIBE SEMANTIC VIEW AD_PERFORMANCE_SEMANTIC_VIEW;

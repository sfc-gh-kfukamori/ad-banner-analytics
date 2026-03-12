"""
広告バナー分析 Streamlit in Snowflake アプリ
=============================================
Snowflake Cortex AI SQL関数を活用した多角的な広告バナー分析ダッシュボード。

機能:
  1. ダッシュボード概要 - KPI、トレンド、セグメント分析
  2. A/Bテスト分析 - テスト結果比較、統計的有意性判定
  3. AI画像分析 - Cortex AI SQL関数によるバナー画像の自動分析
  4. 自然言語クエリ - Cortex Analystによる対話的データ問い合わせ
  5. 改善提案AIアドバイザー - LLMによるクリエイティブ・配信改善提案

デプロイ:
  SiS Warehouse Runtime 向け
  Database: KFUKAMORI_GEN_DB
  Schema: AD_BANNER_ANALYTICS
"""

import json
import streamlit as st
import altair as alt
import pandas as pd
import _snowflake
from snowflake.snowpark.context import get_active_session

# --------------------------------------------------------------------------
# 定数
# --------------------------------------------------------------------------
DB_SCHEMA = "KFUKAMORI_GEN_DB.AD_BANNER_ANALYTICS"
STAGE_NAME = f"@{DB_SCHEMA}.BANNER_IMAGES"
ANALYST_ENDPOINT = "/api/v2/cortex/analyst/message"
ANALYST_TIMEOUT_MS = 60000
SEMANTIC_VIEW = f"{DB_SCHEMA}.AD_PERFORMANCE_SEMANTIC_VIEW"
LLM_MODEL = "claude-3-5-sonnet"
KNOWLEDGE_SEARCH_SERVICE = f"{DB_SCHEMA}.KNOWLEDGE_SEARCH_SERVICE"
KNOWLEDGE_STAGE = f"@{DB_SCHEMA}.KNOWLEDGE_DOCS"

# --------------------------------------------------------------------------
# セッション
# --------------------------------------------------------------------------
session = get_active_session()


# --------------------------------------------------------------------------
# ユーティリティ
# --------------------------------------------------------------------------
def safe_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()


@st.cache_data(ttl=600)
def run_query(sql):
    return session.sql(sql).to_pandas()


def fmt_num(n, decimals=0):
    if n is None:
        return "N/A"
    if decimals == 0:
        return f"{int(n):,}"
    return f"{n:,.{decimals}f}"


def fmt_pct(n, decimals=2):
    if n is None:
        return "N/A"
    return f"{n * 100:.{decimals}f}%"


def delta_pct(cur, prev):
    if prev is None or prev == 0:
        return None
    pct = (cur - prev) / abs(prev) * 100
    return f"{pct:+.1f}%"


def get_presigned_url(image_path: str) -> str:
    """ステージ上のバナー画像のPresigned URLを取得する"""
    try:
        result = session.sql(
            f"SELECT GET_PRESIGNED_URL({STAGE_NAME}, '{image_path}') AS URL"
        ).collect()
        return result[0]["URL"]
    except Exception:
        return None


def display_banner_image(image_path: str, caption: str = None, width: int = None):
    """バナー画像をPresigned URL経由で表示する"""
    url = get_presigned_url(image_path)
    if url:
        if width:
            st.image(url, caption=caption, width=width)
        else:
            st.image(url, caption=caption, use_column_width=True)
    else:
        st.warning(f"画像を取得できません: {image_path}")


def search_knowledge_base(query: str, limit: int = 5) -> list:
    """Cortex Search Serviceでナレッジベースを検索する"""
    try:
        import json as _json
        search_params = _json.dumps({
            "query": query,
            "columns": ["CHUNK_TEXT", "DOC_TITLE", "DOC_FILENAME"],
            "limit": limit
        })
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                '{KNOWLEDGE_SEARCH_SERVICE}',
                '{search_params}'
            ) AS RESULT
        """).collect()
        parsed = json.loads(result[0]["RESULT"])
        return parsed.get("results", [])
    except Exception as e:
        st.warning(f"ナレッジ検索エラー: {e}")
        return []


def format_knowledge_context(results: list) -> str:
    """検索結果をプロンプト用コンテキスト文字列に変換する"""
    if not results:
        return ""
    lines = ["=== 過去の社内分析ナレッジ（類似事例）==="]
    for i, r in enumerate(results, 1):
        title = r.get("DOC_TITLE", "不明")
        text = r.get("CHUNK_TEXT", "")
        lines.append(f"\n【ナレッジ{i}: {title}】")
        lines.append(text)
    return "\n".join(lines)


# --------------------------------------------------------------------------
# データロード
# --------------------------------------------------------------------------
@st.cache_data(ttl=600)
def load_campaigns():
    return run_query(f"SELECT * FROM {DB_SCHEMA}.CAMPAIGNS ORDER BY CAMPAIGN_ID")


@st.cache_data(ttl=600)
def load_banners():
    return run_query(f"SELECT * FROM {DB_SCHEMA}.AD_BANNERS ORDER BY BANNER_ID")


@st.cache_data(ttl=600)
def load_ab_tests():
    return run_query(f"SELECT * FROM {DB_SCHEMA}.AB_TESTS ORDER BY TEST_ID")


@st.cache_data(ttl=600)
def load_ab_results():
    return run_query(f"SELECT * FROM {DB_SCHEMA}.AB_TEST_RESULTS ORDER BY TEST_ID, BANNER_ID")


@st.cache_data(ttl=600)
def load_campaign_summary():
    return run_query(f"SELECT * FROM {DB_SCHEMA}.V_CAMPAIGN_SUMMARY ORDER BY CAMPAIGN_ID")


@st.cache_data(ttl=600)
def load_daily_trend(campaign_id=None):
    if campaign_id:
        return run_query(f"""
            SELECT
                dr.DELIVERY_DATE,
                SUM(dr.IMPRESSIONS) AS IMPRESSIONS,
                SUM(dr.CLICKS) AS CLICKS,
                SUM(dr.CONVERSIONS) AS CONVERSIONS,
                SUM(dr.COST_JPY) AS COST_JPY
            FROM {DB_SCHEMA}.DELIVERY_RESULTS dr
            JOIN {DB_SCHEMA}.AD_BANNERS b ON dr.BANNER_ID = b.BANNER_ID
            WHERE b.CAMPAIGN_ID = {campaign_id}
            GROUP BY dr.DELIVERY_DATE
            ORDER BY dr.DELIVERY_DATE
        """)
    return run_query(f"""
        SELECT
            DELIVERY_DATE,
            SUM(IMPRESSIONS) AS IMPRESSIONS,
            SUM(CLICKS) AS CLICKS,
            SUM(CONVERSIONS) AS CONVERSIONS,
            SUM(COST_JPY) AS COST_JPY
        FROM {DB_SCHEMA}.DELIVERY_RESULTS
        GROUP BY DELIVERY_DATE
        ORDER BY DELIVERY_DATE
    """)


@st.cache_data(ttl=600)
def load_device_summary(campaign_id=None):
    if campaign_id:
        return run_query(f"""
            SELECT
                dr.DEVICE_TYPE,
                SUM(dr.IMPRESSIONS) AS IMPRESSIONS,
                SUM(dr.CLICKS) AS CLICKS,
                SUM(dr.CONVERSIONS) AS CONVERSIONS,
                SUM(dr.COST_JPY) AS COST_JPY
            FROM {DB_SCHEMA}.DELIVERY_RESULTS dr
            JOIN {DB_SCHEMA}.AD_BANNERS b ON dr.BANNER_ID = b.BANNER_ID
            WHERE b.CAMPAIGN_ID = {campaign_id}
            GROUP BY dr.DEVICE_TYPE
            ORDER BY IMPRESSIONS DESC
        """)
    return run_query(f"""
        SELECT
            DEVICE_TYPE,
            SUM(IMPRESSIONS) AS IMPRESSIONS,
            SUM(CLICKS) AS CLICKS,
            SUM(CONVERSIONS) AS CONVERSIONS,
            SUM(COST_JPY) AS COST_JPY
        FROM {DB_SCHEMA}.DELIVERY_RESULTS
        GROUP BY DEVICE_TYPE
        ORDER BY IMPRESSIONS DESC
    """)


@st.cache_data(ttl=600)
def load_region_summary(campaign_id=None):
    if campaign_id:
        return run_query(f"""
            SELECT
                dr.REGION,
                SUM(dr.IMPRESSIONS) AS IMPRESSIONS,
                SUM(dr.CLICKS) AS CLICKS,
                SUM(dr.CONVERSIONS) AS CONVERSIONS,
                SUM(dr.COST_JPY) AS COST_JPY
            FROM {DB_SCHEMA}.DELIVERY_RESULTS dr
            JOIN {DB_SCHEMA}.AD_BANNERS b ON dr.BANNER_ID = b.BANNER_ID
            WHERE b.CAMPAIGN_ID = {campaign_id}
            GROUP BY dr.REGION
            ORDER BY IMPRESSIONS DESC
        """)
    return run_query(f"""
        SELECT
            REGION,
            SUM(IMPRESSIONS) AS IMPRESSIONS,
            SUM(CLICKS) AS CLICKS,
            SUM(CONVERSIONS) AS CONVERSIONS,
            SUM(COST_JPY) AS COST_JPY
        FROM {DB_SCHEMA}.DELIVERY_RESULTS
        GROUP BY REGION
        ORDER BY IMPRESSIONS DESC
    """)


@st.cache_data(ttl=600)
def load_banner_performance(campaign_id=None):
    where_clause = f"WHERE b.CAMPAIGN_ID = {campaign_id}" if campaign_id else ""
    return run_query(f"""
        SELECT
            b.BANNER_ID,
            b.BANNER_NAME,
            b.BANNER_SIZE,
            b.CREATIVE_TYPE,
            b.DESIGN_STYLE,
            b.APPEAL_TYPE,
            c.CAMPAIGN_NAME,
            SUM(dr.IMPRESSIONS) AS IMPRESSIONS,
            SUM(dr.CLICKS) AS CLICKS,
            SUM(dr.CONVERSIONS) AS CONVERSIONS,
            SUM(dr.COST_JPY) AS COST_JPY,
            CASE WHEN SUM(dr.IMPRESSIONS) > 0
                 THEN SUM(dr.CLICKS)::FLOAT / SUM(dr.IMPRESSIONS) ELSE 0 END AS CTR,
            CASE WHEN SUM(dr.CLICKS) > 0
                 THEN SUM(dr.CONVERSIONS)::FLOAT / SUM(dr.CLICKS) ELSE 0 END AS CVR,
            CASE WHEN SUM(dr.CONVERSIONS) > 0
                 THEN SUM(dr.COST_JPY) / SUM(dr.CONVERSIONS) ELSE NULL END AS CPA_JPY
        FROM {DB_SCHEMA}.AD_BANNERS b
        JOIN {DB_SCHEMA}.CAMPAIGNS c ON b.CAMPAIGN_ID = c.CAMPAIGN_ID
        JOIN {DB_SCHEMA}.DELIVERY_RESULTS dr ON b.BANNER_ID = dr.BANNER_ID
        {where_clause}
        GROUP BY b.BANNER_ID, b.BANNER_NAME, b.BANNER_SIZE, b.CREATIVE_TYPE,
                 b.DESIGN_STYLE, b.APPEAL_TYPE, c.CAMPAIGN_NAME
        ORDER BY CTR DESC
    """)


@st.cache_data(ttl=600)
def load_ab_daily(test_id):
    return run_query(f"""
        SELECT
            dr.DELIVERY_DATE,
            dr.BANNER_ID,
            b.BANNER_NAME,
            SUM(dr.IMPRESSIONS) AS IMPRESSIONS,
            SUM(dr.CLICKS) AS CLICKS,
            SUM(dr.CONVERSIONS) AS CONVERSIONS,
            SUM(dr.COST_JPY) AS COST_JPY,
            CASE WHEN SUM(dr.IMPRESSIONS) > 0
                 THEN SUM(dr.CLICKS)::FLOAT / SUM(dr.IMPRESSIONS) ELSE 0 END AS CTR,
            CASE WHEN SUM(dr.CLICKS) > 0
                 THEN SUM(dr.CONVERSIONS)::FLOAT / SUM(dr.CLICKS) ELSE 0 END AS CVR
        FROM {DB_SCHEMA}.DELIVERY_RESULTS dr
        JOIN {DB_SCHEMA}.AD_BANNERS b ON dr.BANNER_ID = b.BANNER_ID
        JOIN {DB_SCHEMA}.AB_TESTS t ON t.TEST_ID = {test_id}
        WHERE dr.BANNER_ID IN (t.BANNER_A_ID, t.BANNER_B_ID)
          AND dr.DELIVERY_DATE BETWEEN t.START_DATE AND COALESCE(t.END_DATE, CURRENT_DATE())
        GROUP BY dr.DELIVERY_DATE, dr.BANNER_ID, b.BANNER_NAME
        ORDER BY dr.DELIVERY_DATE
    """)


# ==========================================================================
# ページ1: ダッシュボード概要
# ==========================================================================
def page_dashboard():
    st.header("ダッシュボード概要")

    # --- キャンペーン選択 ---
    df_campaigns = load_campaigns()
    campaign_options = ["全キャンペーン"] + df_campaigns["CAMPAIGN_NAME"].tolist()
    campaign_ids = [None] + df_campaigns["CAMPAIGN_ID"].tolist()
    selected_idx = st.selectbox(
        "キャンペーン選択",
        range(len(campaign_options)),
        format_func=lambda i: campaign_options[i],
        key="dashboard_campaign_selector"
    )
    selected_campaign_id = campaign_ids[selected_idx]
    if selected_campaign_id:
        st.caption(f"選択中: {campaign_options[selected_idx]}")

    # --- KPI ---
    df_summary = load_campaign_summary()
    if selected_campaign_id:
        df_summary = df_summary[df_summary["CAMPAIGN_ID"] == selected_campaign_id]
    total_imp = df_summary["TOTAL_IMPRESSIONS"].sum()
    total_click = df_summary["TOTAL_CLICKS"].sum()
    total_cv = df_summary["TOTAL_CONVERSIONS"].sum()
    total_cost = df_summary["TOTAL_COST_JPY"].sum()
    overall_ctr = total_click / total_imp if total_imp > 0 else 0
    overall_cvr = total_cv / total_click if total_click > 0 else 0
    overall_cpa = total_cost / total_cv if total_cv > 0 else 0

    cols = st.columns(6)
    with cols[0]:
        st.metric("総インプレッション", fmt_num(total_imp))
    with cols[1]:
        st.metric("総クリック", fmt_num(total_click))
    with cols[2]:
        st.metric("総コンバージョン", fmt_num(total_cv))
    with cols[3]:
        st.metric("総費用", f"¥{fmt_num(total_cost)}")
    with cols[4]:
        st.metric("平均CTR", fmt_pct(overall_ctr))
    with cols[5]:
        st.metric("平均CPA", f"¥{fmt_num(overall_cpa)}")

    # --- 日別トレンド ---
    st.subheader("日別配信トレンド")
    df_daily = load_daily_trend(campaign_id=selected_campaign_id)
    df_daily["CTR"] = df_daily["CLICKS"] / df_daily["IMPRESSIONS"].replace(0, 1)

    metric_choice = st.selectbox(
        "表示指標", ["IMPRESSIONS", "CLICKS", "CONVERSIONS", "COST_JPY", "CTR"],
        format_func=lambda x: {
            "IMPRESSIONS": "インプレッション", "CLICKS": "クリック",
            "CONVERSIONS": "コンバージョン", "COST_JPY": "費用（円）", "CTR": "CTR"
        }.get(x, x)
    )

    chart_daily = (
        alt.Chart(df_daily)
        .mark_area(opacity=0.3, line=True)
        .encode(
            x=alt.X("DELIVERY_DATE:T", title="日付"),
            y=alt.Y(f"{metric_choice}:Q", title=metric_choice),
            tooltip=["DELIVERY_DATE:T", alt.Tooltip(f"{metric_choice}:Q", format=",.2f")],
        )
        .properties(height=300)
    )
    st.altair_chart(chart_daily, use_container_width=True)

    # --- キャンペーン別パフォーマンス ---
    st.subheader("キャンペーン別パフォーマンス")
    df_camp_display = df_summary[
        ["CAMPAIGN_NAME", "ADVERTISER", "OBJECTIVE", "STATUS", "BANNER_COUNT",
         "TOTAL_IMPRESSIONS", "TOTAL_CLICKS", "TOTAL_CONVERSIONS",
         "TOTAL_COST_JPY", "AVG_CTR", "AVG_CVR", "AVG_CPA_JPY", "BUDGET_UTILIZATION_PCT"]
    ].copy()
    df_camp_display["AVG_CTR"] = (df_camp_display["AVG_CTR"] * 100).round(2)
    df_camp_display["AVG_CVR"] = (df_camp_display["AVG_CVR"] * 100).round(2)
    df_camp_display["TOTAL_COST_JPY"] = df_camp_display["TOTAL_COST_JPY"].round(0)
    df_camp_display["AVG_CPA_JPY"] = df_camp_display["AVG_CPA_JPY"].round(0)
    df_camp_display["BUDGET_UTILIZATION_PCT"] = df_camp_display["BUDGET_UTILIZATION_PCT"].round(1)
    df_camp_display.columns = [
        "キャンペーン", "広告主", "目的", "ステータス", "バナー数",
        "imp", "click", "CV", "費用(円)", "CTR(%)", "CVR(%)", "CPA(円)", "予算消化率(%)"
    ]
    st.dataframe(df_camp_display, use_container_width=True)

    # --- デバイス・地域別 ---
    col_d, col_r = st.columns(2)

    with col_d:
        st.subheader("デバイス別内訳")
        df_dev = load_device_summary(campaign_id=selected_campaign_id)
        chart_dev = (
            alt.Chart(df_dev)
            .mark_arc(innerRadius=50)
            .encode(
                theta=alt.Theta("IMPRESSIONS:Q"),
                color=alt.Color("DEVICE_TYPE:N", title="デバイス"),
                tooltip=["DEVICE_TYPE", alt.Tooltip("IMPRESSIONS:Q", format=","),
                          alt.Tooltip("CLICKS:Q", format=","),
                          alt.Tooltip("CONVERSIONS:Q", format=",")],
            )
        )
        st.altair_chart(chart_dev, use_container_width=True)

    with col_r:
        st.subheader("地域別内訳")
        df_reg = load_region_summary(campaign_id=selected_campaign_id)
        df_reg["CTR"] = df_reg["CLICKS"] / df_reg["IMPRESSIONS"].replace(0, 1) * 100
        chart_reg = (
            alt.Chart(df_reg)
            .mark_bar()
            .encode(
                x=alt.X("IMPRESSIONS:Q", title="インプレッション"),
                y=alt.Y("REGION:N", title="地域", sort="-x"),
                color=alt.Color("CTR:Q", title="CTR(%)", scale=alt.Scale(scheme="blues")),
                tooltip=["REGION",
                          alt.Tooltip("IMPRESSIONS:Q", format=","),
                          alt.Tooltip("CLICKS:Q", format=","),
                          alt.Tooltip("CTR:Q", format=".2f")],
            )
        )
        st.altair_chart(chart_reg, use_container_width=True)

    # --- バナー別ランキング ---
    st.subheader("バナー別パフォーマンスランキング")
    df_bp = load_banner_performance(campaign_id=selected_campaign_id)
    df_bp["CTR_PCT"] = df_bp["CTR"] * 100
    df_bp["CVR_PCT"] = df_bp["CVR"] * 100

    sort_by = st.selectbox(
        "ソート基準",
        ["CTR_PCT", "CVR_PCT", "CPA_JPY", "IMPRESSIONS", "CONVERSIONS"],
        format_func=lambda x: {
            "CTR_PCT": "CTR", "CVR_PCT": "CVR", "CPA_JPY": "CPA",
            "IMPRESSIONS": "imp", "CONVERSIONS": "CV"
        }.get(x, x)
    )
    ascending = sort_by == "CPA_JPY"

    chart_banner_rank = (
        alt.Chart(df_bp.sort_values(sort_by, ascending=ascending).head(15))
        .mark_bar()
        .encode(
            x=alt.X(f"{sort_by}:Q", title=sort_by.replace("_PCT", "(%)")),
            y=alt.Y("BANNER_NAME:N", title="バナー", sort="-x" if not ascending else "x"),
            color=alt.Color("CAMPAIGN_NAME:N", title="キャンペーン"),
            tooltip=["BANNER_NAME", "CAMPAIGN_NAME",
                      alt.Tooltip("CTR_PCT:Q", format=".3f", title="CTR(%)"),
                      alt.Tooltip("CVR_PCT:Q", format=".2f", title="CVR(%)"),
                      alt.Tooltip("CPA_JPY:Q", format=",.0f", title="CPA(円)")],
        )
        .properties(height=400)
    )
    st.altair_chart(chart_banner_rank, use_container_width=True)


# ==========================================================================
# ページ2: A/Bテスト分析
# ==========================================================================
def page_ab_test():
    st.header("A/Bテスト分析")

    df_tests = load_ab_tests()
    df_results = load_ab_results()
    df_banners = load_banners()

    # --- テスト一覧 ---
    st.subheader("テスト一覧")
    test_display = df_tests.merge(
        df_banners[["BANNER_ID", "BANNER_NAME"]].rename(
            columns={"BANNER_ID": "BANNER_A_ID", "BANNER_NAME": "BANNER_A_NAME"}
        ), on="BANNER_A_ID", how="left"
    ).merge(
        df_banners[["BANNER_ID", "BANNER_NAME"]].rename(
            columns={"BANNER_ID": "BANNER_B_ID", "BANNER_NAME": "BANNER_B_NAME"}
        ), on="BANNER_B_ID", how="left"
    )

    for _, row in test_display.iterrows():
        status_icon = {"COMPLETED": "DONE", "RUNNING": "LIVE", "STOPPED": "STOP"}.get(row["STATUS"], "?")
        winner_text = ""
        if row["WINNER_BANNER_ID"]:
            winner_name = df_banners[df_banners["BANNER_ID"] == row["WINNER_BANNER_ID"]]["BANNER_NAME"].values
            winner_text = f" | Winner: {winner_name[0]}" if len(winner_name) > 0 else ""
            if row["CONFIDENCE_LEVEL"]:
                winner_text += f" (信頼度: {row['CONFIDENCE_LEVEL']}%)"
        st.markdown(
            f"**[{status_icon}] {row['TEST_NAME']}** - テスト変数: {row['TEST_VARIABLE']}"
            f"{winner_text}"
        )

    # --- テスト選択と詳細 ---
    st.divider()
    st.subheader("テスト詳細分析")
    selected_test = st.selectbox(
        "分析するテストを選択",
        df_tests["TEST_ID"].tolist(),
        format_func=lambda tid: df_tests[df_tests["TEST_ID"] == tid]["TEST_NAME"].values[0]
    )

    test_info = df_tests[df_tests["TEST_ID"] == selected_test].iloc[0]
    test_results = df_results[df_results["TEST_ID"] == selected_test]

    if len(test_results) == 0:
        st.warning("このテストの結果データがありません。")
        return

    # バナーA/B情報
    banner_a = df_banners[df_banners["BANNER_ID"] == test_info["BANNER_A_ID"]]
    banner_b = df_banners[df_banners["BANNER_ID"] == test_info["BANNER_B_ID"]]
    result_a = test_results[test_results["BANNER_ID"] == test_info["BANNER_A_ID"]]
    result_b = test_results[test_results["BANNER_ID"] == test_info["BANNER_B_ID"]]

    if len(result_a) == 0 or len(result_b) == 0:
        st.warning("A/B両方の結果データが揃っていません。")
        return

    ra = result_a.iloc[0]
    rb = result_b.iloc[0]
    ba = banner_a.iloc[0] if len(banner_a) > 0 else None
    bb = banner_b.iloc[0] if len(banner_b) > 0 else None

    # --- 比較カード ---
    col_a, col_vs, col_b = st.columns([5, 1, 5])

    with col_a:
        st.markdown(f"### Banner A: {ba['BANNER_NAME'] if ba is not None else 'N/A'}")
        if ba is not None:
            display_banner_image(ba["IMAGE_PATH"], caption=ba["BANNER_NAME"])
            st.caption(f"サイズ: {ba['BANNER_SIZE']} | スタイル: {ba['DESIGN_STYLE']} | 訴求: {ba['APPEAL_TYPE']}")
            st.caption(f"CTA: 「{ba['CTA_TEXT']}」")
        st.metric("CTR", fmt_pct(ra["CTR"]))
        st.metric("CVR", fmt_pct(ra["CVR"]))
        st.metric("CPA", f"¥{fmt_num(ra['CPA_JPY'])}")
        st.metric("総imp", fmt_num(ra["TOTAL_IMPRESSIONS"]))
        st.metric("総CV", fmt_num(ra["TOTAL_CONVERSIONS"]))

    with col_vs:
        st.markdown("<div style='text-align:center; padding-top:80px; font-size:24px; font-weight:bold;'>VS</div>",
                    unsafe_allow_html=True)

    with col_b:
        st.markdown(f"### Banner B: {bb['BANNER_NAME'] if bb is not None else 'N/A'}")
        if bb is not None:
            display_banner_image(bb["IMAGE_PATH"], caption=bb["BANNER_NAME"])
            st.caption(f"サイズ: {bb['BANNER_SIZE']} | スタイル: {bb['DESIGN_STYLE']} | 訴求: {bb['APPEAL_TYPE']}")
            st.caption(f"CTA: 「{bb['CTA_TEXT']}」")
        st.metric("CTR", fmt_pct(rb["CTR"]))
        st.metric("CVR", fmt_pct(rb["CVR"]))
        st.metric("CPA", f"¥{fmt_num(rb['CPA_JPY'])}")
        st.metric("総imp", fmt_num(rb["TOTAL_IMPRESSIONS"]))
        st.metric("総CV", fmt_num(rb["TOTAL_CONVERSIONS"]))

    # --- 統計的有意性 ---
    st.divider()
    st.subheader("統計的有意性")
    sig = ra.get("STATISTICAL_SIGNIFICANCE", "INCONCLUSIVE")
    conf = test_info.get("CONFIDENCE_LEVEL")
    p_val = ra.get("P_VALUE")

    if sig == "SIGNIFICANT":
        st.success(f"統計的に有意な差があります（信頼度: {conf}%, p値: {p_val:.4f}）")
    elif sig == "MARGINALLY_SIGNIFICANT":
        st.warning(f"やや有意な差があります（信頼度: {conf}%, p値: {p_val:.4f}）")
    elif sig == "INCONCLUSIVE":
        st.info("テスト進行中 - まだ結論を出すには早い段階です")
    else:
        st.error(f"有意な差は検出されませんでした（信頼度: {conf}%, p値: {p_val:.4f}）")

    # --- 日別推移比較 ---
    st.subheader("日別CTR推移比較")
    df_ab_daily = load_ab_daily(selected_test)
    if len(df_ab_daily) > 0:
        chart_ab = (
            alt.Chart(df_ab_daily)
            .mark_line(point=True)
            .encode(
                x=alt.X("DELIVERY_DATE:T", title="日付"),
                y=alt.Y("CTR:Q", title="CTR"),
                color=alt.Color("BANNER_NAME:N", title="バナー"),
                tooltip=["DELIVERY_DATE:T", "BANNER_NAME:N",
                          alt.Tooltip("CTR:Q", format=".4f"),
                          alt.Tooltip("IMPRESSIONS:Q", format=",")],
            )
            .properties(height=300)
        )
        st.altair_chart(chart_ab, use_container_width=True)

    # --- テスト変数別勝敗傾向 ---
    st.subheader("テスト変数別 勝敗傾向")
    completed_tests = df_tests[df_tests["STATUS"] == "COMPLETED"]
    if len(completed_tests) > 0:
        variable_results = []
        for _, t in completed_tests.iterrows():
            winner_id = t["WINNER_BANNER_ID"]
            if winner_id:
                winner_info = df_banners[df_banners["BANNER_ID"] == winner_id]
                if len(winner_info) > 0:
                    variable_results.append({
                        "テスト変数": t["TEST_VARIABLE"],
                        "テスト名": t["TEST_NAME"],
                        "勝者バナー": winner_info.iloc[0]["BANNER_NAME"],
                        "勝者スタイル": winner_info.iloc[0]["DESIGN_STYLE"],
                        "勝者訴求": winner_info.iloc[0]["APPEAL_TYPE"],
                        "信頼度(%)": t["CONFIDENCE_LEVEL"],
                    })
        if variable_results:
            st.dataframe(pd.DataFrame(variable_results), use_container_width=True)


# ==========================================================================
# ページ3: AI画像分析
# ==========================================================================
def page_ai_image():
    st.header("AI画像分析 (Cortex AI SQL関数)")
    st.caption(
        "Snowflake Cortex AI SQL関数を使用して、バナー画像のクリエイティブ要素を自動分析します。"
        "画像がステージにアップロードされている場合、AI_COMPLETE / AI_CLASSIFY / AI_SIMILARITY が利用可能です。"
    )

    df_banners = load_banners()
    df_bp = load_banner_performance()

    # --- 画像ステージ確認 ---
    st.subheader("1. 画像ステージ状況")
    try:
        stage_files = run_query(f"SELECT * FROM DIRECTORY({STAGE_NAME})")
        if len(stage_files) > 0:
            st.success(f"ステージに {len(stage_files)} ファイルが見つかりました")
            st.dataframe(stage_files, use_container_width=True)
            has_images = True
        else:
            st.info(
                "ステージにファイルがありません。バナー画像をアップロードすると、"
                "AI画像分析機能が有効になります。\n\n"
                f"アップロード先: `{STAGE_NAME}`\n\n"
                "```sql\n"
                f"PUT file:///path/to/banner.png {STAGE_NAME}/campaign1/;\n"
                "```"
            )
            has_images = False
    except Exception:
        st.info("ステージへのアクセスを確認中... 画像がない場合はメタデータベースの分析を行います。")
        has_images = False

    # --- AI画像分析 (画像がある場合) ---
    if has_images:
        st.divider()
        st.subheader("2. AI_COMPLETE - クリエイティブレビュー")
        st.caption("LLMがバナー画像の視覚的要素を分析し、広告効果の観点から評価します")

        selected_banner_for_review = st.selectbox(
            "分析するバナーを選択（AI_COMPLETE）",
            df_banners["BANNER_ID"].tolist(),
            format_func=lambda bid: df_banners[df_banners["BANNER_ID"] == bid]["BANNER_NAME"].values[0],
            key="ai_complete_banner"
        )
        banner_row = df_banners[df_banners["BANNER_ID"] == selected_banner_for_review].iloc[0]

        display_banner_image(banner_row["IMAGE_PATH"], caption=banner_row["BANNER_NAME"], width=400)

        if st.button("クリエイティブレビュー実行", key="run_creative_review"):
            with st.spinner("AI_COMPLETEで画像を分析中..."):
                try:
                    review_sql = f"""
                        SELECT AI_COMPLETE(
                            '{LLM_MODEL}',
                            'この広告バナー画像を以下の観点で詳細に分析してください（日本語で回答）:
                            1. 色彩構成とブランドイメージへの影響
                            2. レイアウト構成とユーザーの視線誘導
                            3. テキストの可読性とメッセージの明確さ
                            4. CTAボタンの視認性と訴求力
                            5. 全体的な広告効果の評価（5段階）
                            6. 具体的な改善提案（3つ以上）',
                            TO_FILE('{STAGE_NAME}', '{banner_row["IMAGE_PATH"]}')
                        ) AS REVIEW
                    """
                    result = run_query(review_sql)
                    review_text = result.iloc[0]["REVIEW"]
                    # \nリテラルを実際の改行に変換して表示
                    st.markdown(review_text.replace("\\n", "\n"))

                    # キャッシュに保存（Python側でエスケープしOBJECT_CONSTRUCTで安全に保存）
                    try:
                        escaped_review = review_text.replace("'", "''")
                        session.sql(f"""
                            INSERT INTO {DB_SCHEMA}.BANNER_AI_ANALYSIS
                            (BANNER_ID, ANALYSIS_TYPE, MODEL_USED, ANALYSIS_RESULT)
                            SELECT {selected_banner_for_review}, 'CREATIVE_REVIEW', '{LLM_MODEL}',
                                   OBJECT_CONSTRUCT('review', '{escaped_review}')
                        """).collect()
                    except Exception:
                        pass  # キャッシュ保存失敗は無視
                except Exception as e:
                    st.error(f"AI_COMPLETE実行エラー: {e}")

        st.divider()
        st.subheader("3. AI_CLASSIFY - バナー自動分類")
        st.caption("AI_CLASSIFYで画像から訴求タイプやトーンを自動判定します")

        selected_banner_for_classify = st.selectbox(
            "分類するバナーを選択",
            df_banners["BANNER_ID"].tolist(),
            format_func=lambda bid: df_banners[df_banners["BANNER_ID"] == bid]["BANNER_NAME"].values[0],
            key="ai_classify_banner"
        )
        banner_cls = df_banners[df_banners["BANNER_ID"] == selected_banner_for_classify].iloc[0]

        display_banner_image(banner_cls["IMAGE_PATH"], caption=banner_cls["BANNER_NAME"], width=400)

        col_cls1, col_cls2 = st.columns(2)
        with col_cls1:
            if st.button("訴求タイプ判定", key="classify_appeal"):
                with st.spinner("AI_CLASSIFYで分類中..."):
                    try:
                        cls_sql = f"""
                            SELECT AI_CLASSIFY(
                                TO_FILE('{STAGE_NAME}', '{banner_cls["IMAGE_PATH"]}'),
                                ['価格訴求 - セール・割引を前面に出した広告',
                                 'ブランド訴求 - ブランドイメージ・世界観を表現',
                                 '機能訴求 - 製品スペック・機能を訴求',
                                 '感情訴求 - 感情に訴えかけるストーリー性のある広告']
                            ) AS CLASSIFICATION
                        """
                        result = run_query(cls_sql)
                        st.json(result.iloc[0]["CLASSIFICATION"])
                    except Exception as e:
                        st.error(f"AI_CLASSIFY実行エラー: {e}")

        with col_cls2:
            if st.button("トーン判定", key="classify_tone"):
                with st.spinner("AI_CLASSIFYで分類中..."):
                    try:
                        tone_sql = f"""
                            SELECT AI_CLASSIFY(
                                TO_FILE('{STAGE_NAME}', '{banner_cls["IMAGE_PATH"]}'),
                                ['フォーマル・高級感', 'カジュアル・親しみやすい',
                                 'エモーショナル・感動的', 'ポップ・明るい・元気']
                            ) AS TONE
                        """
                        result = run_query(tone_sql)
                        st.json(result.iloc[0]["TONE"])
                    except Exception as e:
                        st.error(f"AI_CLASSIFY実行エラー: {e}")

        st.divider()
        st.subheader("4. AI_SIMILARITY - バナー類似度分析")
        st.caption("2つのバナー間の視覚的類似度をベクトル埋め込みで計算します")

        col_sim1, col_sim2 = st.columns(2)
        with col_sim1:
            sim_banner_a = st.selectbox(
                "バナーA",
                df_banners["BANNER_ID"].tolist(),
                format_func=lambda bid: df_banners[df_banners["BANNER_ID"] == bid]["BANNER_NAME"].values[0],
                key="sim_a"
            )
            sim_a_row = df_banners[df_banners["BANNER_ID"] == sim_banner_a].iloc[0]
            display_banner_image(sim_a_row["IMAGE_PATH"], caption=sim_a_row["BANNER_NAME"])
        with col_sim2:
            sim_banner_b = st.selectbox(
                "バナーB",
                df_banners["BANNER_ID"].tolist(),
                index=min(1, len(df_banners) - 1),
                format_func=lambda bid: df_banners[df_banners["BANNER_ID"] == bid]["BANNER_NAME"].values[0],
                key="sim_b"
            )
            sim_b_row = df_banners[df_banners["BANNER_ID"] == sim_banner_b].iloc[0]
            display_banner_image(sim_b_row["IMAGE_PATH"], caption=sim_b_row["BANNER_NAME"])

        if st.button("類似度計算", key="calc_similarity"):
            ba_path = df_banners[df_banners["BANNER_ID"] == sim_banner_a].iloc[0]["IMAGE_PATH"]
            bb_path = df_banners[df_banners["BANNER_ID"] == sim_banner_b].iloc[0]["IMAGE_PATH"]
            with st.spinner("AI_EMBEDでベクトル化し類似度を計算中..."):
                try:
                    sim_sql = f"""
                        WITH embeddings AS (
                            SELECT
                                AI_EMBED('voyage-multimodal-3',
                                    TO_FILE('{STAGE_NAME}', '{ba_path}')) AS emb_a,
                                AI_EMBED('voyage-multimodal-3',
                                    TO_FILE('{STAGE_NAME}', '{bb_path}')) AS emb_b
                        )
                        SELECT VECTOR_COSINE_SIMILARITY(emb_a, emb_b) AS SIMILARITY
                        FROM embeddings
                    """
                    result = run_query(sim_sql)
                    sim_score = result.iloc[0]["SIMILARITY"]
                    st.metric("類似度スコア", f"{sim_score:.4f}")
                    if sim_score >= 0.8:
                        st.info("非常に類似したクリエイティブです。差別化が必要かもしれません。")
                    elif sim_score >= 0.5:
                        st.success("適度な差異があります。A/Bテストに適した組み合わせです。")
                    else:
                        st.warning("大きく異なるクリエイティブです。テスト変数の特定が難しい可能性があります。")
                except Exception as e:
                    st.error(f"AI_SIMILARITY計算エラー: {e}")

        st.divider()
        st.subheader("5. AI_COMPLETE (PROMPT) - バナー比較分析")
        st.caption("2つの画像を同時にLLMに渡し、比較分析を行います")

        col_cmp1, col_cmp2 = st.columns(2)
        with col_cmp1:
            cmp_a = st.selectbox("比較バナーA", df_banners["BANNER_ID"].tolist(),
                                 format_func=lambda bid: df_banners[df_banners["BANNER_ID"] == bid]["BANNER_NAME"].values[0],
                                 key="cmp_a")
            cmp_a_row = df_banners[df_banners["BANNER_ID"] == cmp_a].iloc[0]
            display_banner_image(cmp_a_row["IMAGE_PATH"], caption=cmp_a_row["BANNER_NAME"])
        with col_cmp2:
            cmp_b = st.selectbox("比較バナーB", df_banners["BANNER_ID"].tolist(),
                                 index=min(1, len(df_banners) - 1),
                                 format_func=lambda bid: df_banners[df_banners["BANNER_ID"] == bid]["BANNER_NAME"].values[0],
                                 key="cmp_b")
            cmp_b_row = df_banners[df_banners["BANNER_ID"] == cmp_b].iloc[0]
            display_banner_image(cmp_b_row["IMAGE_PATH"], caption=cmp_b_row["BANNER_NAME"])

        if st.button("比較分析実行", key="run_compare"):
            path_a = df_banners[df_banners["BANNER_ID"] == cmp_a].iloc[0]["IMAGE_PATH"]
            path_b = df_banners[df_banners["BANNER_ID"] == cmp_b].iloc[0]["IMAGE_PATH"]
            with st.spinner("2つのバナーをAI_COMPLETEで比較中..."):
                try:
                    compare_sql = f"""
                        SELECT AI_COMPLETE('{LLM_MODEL}',
                            PROMPT('以下の2つの広告バナーを比較分析してください（日本語で回答）:
                            画像1: {{0}}
                            画像2: {{1}}

                            分析項目:
                            1. 視覚的な違い（色彩、レイアウト、イメージ）
                            2. メッセージ・訴求の違い
                            3. 想定ターゲット層の違い
                            4. それぞれの強みと弱み
                            5. どちらがより高いCTRが期待できるか、その理由',
                            TO_FILE('{STAGE_NAME}', '{path_a}'),
                            TO_FILE('{STAGE_NAME}', '{path_b}')
                        )) AS COMPARISON
                    """
                    result = run_query(compare_sql)
                    st.markdown(result.iloc[0]["COMPARISON"].replace("\\n", "\n"))
                except Exception as e:
                    st.error(f"比較分析エラー: {e}")

    # --- メタデータベースの分析 (画像がなくても実行可能) ---
    st.divider()
    st.subheader("6. メタデータベース分析")
    st.caption("バナーのメタ情報（デザインスタイル、訴求タイプ等）とパフォーマンスの相関を分析します")

    # デザインスタイル別パフォーマンス
    col_meta1, col_meta2 = st.columns(2)

    with col_meta1:
        st.markdown("**デザインスタイル別 平均CTR**")
        style_perf = df_bp.groupby("DESIGN_STYLE").agg(
            AVG_CTR=("CTR", "mean"),
            AVG_CVR=("CVR", "mean"),
            BANNER_COUNT=("BANNER_ID", "count")
        ).reset_index()
        style_perf["AVG_CTR_PCT"] = style_perf["AVG_CTR"] * 100

        chart_style = (
            alt.Chart(style_perf)
            .mark_bar()
            .encode(
                x=alt.X("AVG_CTR_PCT:Q", title="平均CTR(%)"),
                y=alt.Y("DESIGN_STYLE:N", title="デザインスタイル", sort="-x"),
                color=alt.Color("DESIGN_STYLE:N", legend=None),
                tooltip=["DESIGN_STYLE",
                          alt.Tooltip("AVG_CTR_PCT:Q", format=".3f", title="CTR(%)"),
                          "BANNER_COUNT"],
            )
        )
        st.altair_chart(chart_style, use_container_width=True)

    with col_meta2:
        st.markdown("**訴求タイプ別 平均CTR**")
        appeal_perf = df_bp.groupby("APPEAL_TYPE").agg(
            AVG_CTR=("CTR", "mean"),
            AVG_CVR=("CVR", "mean"),
            BANNER_COUNT=("BANNER_ID", "count")
        ).reset_index()
        appeal_perf["AVG_CTR_PCT"] = appeal_perf["AVG_CTR"] * 100

        chart_appeal = (
            alt.Chart(appeal_perf)
            .mark_bar()
            .encode(
                x=alt.X("AVG_CTR_PCT:Q", title="平均CTR(%)"),
                y=alt.Y("APPEAL_TYPE:N", title="訴求タイプ", sort="-x"),
                color=alt.Color("APPEAL_TYPE:N", legend=None),
                tooltip=["APPEAL_TYPE",
                          alt.Tooltip("AVG_CTR_PCT:Q", format=".3f", title="CTR(%)"),
                          "BANNER_COUNT"],
            )
        )
        st.altair_chart(chart_appeal, use_container_width=True)

    # CTAテキスト別
    st.markdown("**CTAテキスト別パフォーマンス**")
    cta_perf = df_bp.groupby("CAMPAIGN_NAME").apply(
        lambda g: pd.Series({
            "BANNER_NAME": g.loc[g["CTR"].idxmax(), "BANNER_NAME"],
            "CTA": load_banners().set_index("BANNER_ID").loc[g.loc[g["CTR"].idxmax(), "BANNER_ID"], "CTA_TEXT"],
            "CTR(%)": g["CTR"].max() * 100,
            "CVR(%)": g.loc[g["CTR"].idxmax(), "CVR"] * 100,
        })
    ).reset_index()
    st.dataframe(cta_perf, use_container_width=True)

    # ==========================================================================
    # セクション 7: クリエイティブ要素抽出 & パフォーマンス相関分析
    # ==========================================================================
    if has_images:
        st.divider()
        st.subheader("7. クリエイティブ要素抽出 & パフォーマンス相関")
        st.caption(
            "AI_COMPLETEでバナー画像から視覚要素をJSON構造化抽出し、"
            "配信パフォーマンスとの相関を分析します"
        )

        if st.button("全バナー要素抽出を実行", key="run_element_extraction"):
            with st.spinner("AI_COMPLETEで各バナーの視覚要素を抽出中...（時間がかかる場合があります）"):
                elements_list = []
                errors = []
                progress = st.progress(0)
                total = len(df_banners)
                for idx, (_, brow) in enumerate(df_banners.iterrows()):
                    try:
                        extract_sql = f"""
                            SELECT AI_COMPLETE(
                                '{LLM_MODEL}',
                                'この広告バナー画像を分析し、以下のJSON形式のみで回答してください。説明文は不要です。
{{"dominant_color":"メインカラー名","color_warmth":"暖色/寒色/中性色","has_person":"true/false","text_amount":"多い/普通/少ない","layout":"左寄せ/中央/右寄せ/分割","emotional_tone":"安心/興奮/高級/親しみ/クール","cta_prominence":"高/中/低","visual_complexity":"シンプル/標準/複雑"}}',
                                TO_FILE('{STAGE_NAME}', '{brow["IMAGE_PATH"]}')
                            ) AS ELEMENTS
                        """
                        res = session.sql(extract_sql).to_pandas()
                        raw = res.iloc[0]["ELEMENTS"].replace("\\n", "").strip()
                        # JSON部分を抽出
                        start = raw.find("{")
                        end = raw.rfind("}") + 1
                        if start >= 0 and end > start:
                            parsed = json.loads(raw[start:end])
                            parsed["BANNER_ID"] = brow["BANNER_ID"]
                            parsed["BANNER_NAME"] = brow["BANNER_NAME"]
                            elements_list.append(parsed)
                        else:
                            errors.append(f"{brow['BANNER_NAME']}: JSONが見つかりません")
                    except Exception as e:
                        errors.append(f"{brow['BANNER_NAME']}: {str(e)[:80]}")
                    progress.progress((idx + 1) / total)

                if elements_list:
                    df_elements = pd.DataFrame(elements_list)
                    st.session_state["extracted_elements"] = df_elements
                    st.success(f"{len(elements_list)}件のバナーから要素を抽出しました")
                else:
                    st.session_state["extracted_elements"] = None
                    st.error("要素を抽出できたバナーが0件でした")

                if errors:
                    with st.expander(f"エラー詳細 ({len(errors)}件)", expanded=False):
                        for err in errors:
                            st.caption(err)

        # 結果表示（セッションに保存済みの場合も表示）
        if st.session_state.get("extracted_elements") is not None:
            df_elements = st.session_state["extracted_elements"]
            st.markdown("**抽出された視覚要素:**")
            st.dataframe(df_elements, use_container_width=True)

            # パフォーマンスと結合して相関分析
            df_merged = df_elements.merge(
                df_bp[["BANNER_ID", "CTR", "CVR", "IMPRESSIONS"]],
                on="BANNER_ID", how="left"
            )

            # 要素別CTR比較チャート
            for col_name in ["color_warmth", "has_person", "text_amount",
                             "emotional_tone", "cta_prominence", "visual_complexity"]:
                if col_name in df_merged.columns:
                    grp = df_merged.groupby(col_name).agg(
                        AVG_CTR=("CTR", "mean"),
                        COUNT=("BANNER_ID", "count")
                    ).reset_index()
                    grp["AVG_CTR_PCT"] = grp["AVG_CTR"] * 100

                    if len(grp) > 1:
                        chart_elem = (
                            alt.Chart(grp)
                            .mark_bar()
                            .encode(
                                x=alt.X("AVG_CTR_PCT:Q", title="平均CTR(%)"),
                                y=alt.Y(f"{col_name}:N", sort="-x"),
                                color=alt.Color(f"{col_name}:N", legend=None),
                                tooltip=[col_name,
                                         alt.Tooltip("AVG_CTR_PCT:Q", format=".3f"),
                                         "COUNT"],
                            )
                            .properties(title=f"{col_name} 別 平均CTR", height=150)
                        )
                        st.altair_chart(chart_elem, use_container_width=True)

            # AI による総合分析
            st.markdown("**AI総合相関分析:**")
            try:
                merged_text = df_merged.to_string(index=False)
                corr_prompt = (
                    "以下は広告バナーの視覚要素とパフォーマンス（CTR, CVR）のデータです。"
                    "どの視覚要素がCTR/CVRに最も影響しているか、日本語で分析してください。"
                    "具体的な数値を引用し、実用的な示唆を3〜5点挙げてください。\n\n"
                    f"{merged_text}"
                ).replace("'", "''")
                corr_sql = f"SELECT AI_COMPLETE('{LLM_MODEL}', '{corr_prompt}') AS ANALYSIS"
                corr_result = session.sql(corr_sql).to_pandas()
                st.markdown(corr_result.iloc[0]["ANALYSIS"].replace("\\n", "\n"))
            except Exception as e:
                st.error(f"相関分析エラー: {e}")

    # ==========================================================================
    # セクション 8: 全バナー一括 品質・ターゲット分類
    # ==========================================================================
    if has_images:
        st.divider()
        st.subheader("8. バナー品質 & ターゲット一括分類")
        st.caption(
            "AI_CLASSIFYで全バナーの品質グレードとターゲット年齢層を一括判定します"
        )

        if st.button("一括分類を実行", key="run_batch_classify"):
            with st.spinner("AI_CLASSIFYで全バナーを一括分類中..."):
                classify_results = []
                progress2 = st.progress(0)
                total2 = len(df_banners)
                for idx, (_, brow) in enumerate(df_banners.iterrows()):
                    row_result = {
                        "BANNER_ID": brow["BANNER_ID"],
                        "BANNER_NAME": brow["BANNER_NAME"],
                    }
                    # 品質分類
                    try:
                        q_sql = f"""
                            SELECT AI_CLASSIFY(
                                TO_FILE('{STAGE_NAME}', '{brow["IMAGE_PATH"]}'),
                                ['高品質プロフェッショナル', '標準品質', '改善が必要']
                            ) AS QUALITY
                        """
                        q_res = session.sql(q_sql).to_pandas()
                        q_json = json.loads(q_res.iloc[0]["QUALITY"]) if isinstance(q_res.iloc[0]["QUALITY"], str) else q_res.iloc[0]["QUALITY"]
                        row_result["品質グレード"] = q_json.get("label", "N/A")
                        row_result["品質スコア"] = round(q_json.get("score", 0), 3)
                    except Exception:
                        row_result["品質グレード"] = "判定不可"
                        row_result["品質スコア"] = 0

                    # ターゲット年齢層分類
                    try:
                        t_sql = f"""
                            SELECT AI_CLASSIFY(
                                TO_FILE('{STAGE_NAME}', '{brow["IMAGE_PATH"]}'),
                                ['10-20代向け', '30-40代向け', '50代以上向け', '全年齢向け']
                            ) AS TARGET_AGE
                        """
                        t_res = session.sql(t_sql).to_pandas()
                        t_json = json.loads(t_res.iloc[0]["TARGET_AGE"]) if isinstance(t_res.iloc[0]["TARGET_AGE"], str) else t_res.iloc[0]["TARGET_AGE"]
                        row_result["ターゲット年齢層"] = t_json.get("label", "N/A")
                        row_result["年齢スコア"] = round(t_json.get("score", 0), 3)
                    except Exception:
                        row_result["ターゲット年齢層"] = "判定不可"
                        row_result["年齢スコア"] = 0

                    classify_results.append(row_result)
                    progress2.progress((idx + 1) / total2)

                if classify_results:
                    df_classify = pd.DataFrame(classify_results)
                    st.session_state["batch_classify"] = df_classify

        if st.session_state.get("batch_classify") is not None:
            df_cls = st.session_state["batch_classify"]
            st.dataframe(df_cls, use_container_width=True)

            # 改善が必要なバナーをハイライト
            needs_improvement = df_cls[df_cls["品質グレード"] == "改善が必要"]
            if len(needs_improvement) > 0:
                st.warning(f"改善が必要と判定されたバナー: {len(needs_improvement)}件")
                for _, r in needs_improvement.iterrows():
                    st.markdown(f"- **{r['BANNER_NAME']}** (品質スコア: {r['品質スコア']})")

    # ==========================================================================
    # セクション 9: AI_FILTER — 自然言語バナー絞り込み
    # ==========================================================================
    if has_images:
        st.divider()
        st.subheader("9. AI_FILTER - 自然言語バナー絞り込み")
        st.caption(
            "自然言語の条件を指定して、マッチするバナーをAI_FILTERで自動抽出します"
        )

        # プリセット条件
        filter_presets = [
            "暖色系の配色を使ったバナー",
            "人物やキャラクターが含まれるバナー",
            "テキストが多めのバナー",
            "高級感のあるデザインのバナー",
            "シンプルで視認性の高いバナー",
        ]
        preset_filter = st.selectbox(
            "プリセット条件を選択（または下の自由入力を使用）",
            ["（選択してください）"] + filter_presets,
            key="filter_preset"
        )

        custom_filter = st.text_input(
            "自由入力: フィルタ条件を日本語で入力",
            placeholder="例: CTAボタンが目立つバナー",
            key="custom_filter_input"
        )

        filter_condition = custom_filter if custom_filter else (
            preset_filter if preset_filter != "（選択してください）" else None
        )

        if st.button("バナー絞り込み実行", key="run_ai_filter") and filter_condition:
            with st.spinner(f"AI_FILTERで「{filter_condition}」に該当するバナーを検索中..."):
                matched_banners = []
                progress3 = st.progress(0)
                total3 = len(df_banners)
                for idx, (_, brow) in enumerate(df_banners.iterrows()):
                    try:
                        filter_sql = f"""
                            SELECT AI_FILTER(
                                TO_FILE('{STAGE_NAME}', '{brow["IMAGE_PATH"]}'),
                                '{filter_condition.replace("'", "''")}'
                            ) AS IS_MATCH
                        """
                        f_res = session.sql(filter_sql).to_pandas()
                        if f_res.iloc[0]["IS_MATCH"]:
                            matched_banners.append(brow)
                    except Exception:
                        pass
                    progress3.progress((idx + 1) / total3)

                st.session_state["filter_results"] = matched_banners
                st.session_state["filter_condition"] = filter_condition

        if st.session_state.get("filter_results") is not None:
            matched = st.session_state["filter_results"]
            cond = st.session_state.get("filter_condition", "")
            st.markdown(f"**「{cond}」に該当: {len(matched)}件 / {len(df_banners)}件**")

            if matched:
                # グリッド表示
                cols_per_row = 3
                for i in range(0, len(matched), cols_per_row):
                    grid_cols = st.columns(cols_per_row)
                    for j, col in enumerate(grid_cols):
                        if i + j < len(matched):
                            b = matched[i + j]
                            with col:
                                display_banner_image(b["IMAGE_PATH"], caption=b["BANNER_NAME"])
                                # パフォーマンス情報
                                perf = df_bp[df_bp["BANNER_ID"] == b["BANNER_ID"]]
                                if len(perf) > 0:
                                    p = perf.iloc[0]
                                    st.caption(
                                        f"CTR: {fmt_pct(p['CTR'])} | CVR: {fmt_pct(p['CVR'])}"
                                    )
            else:
                st.info("該当するバナーが見つかりませんでした。条件を変更してお試しください。")

    # ==========================================================================
    # セクション 10: キャンペーン横断 バナー効果予測
    # ==========================================================================
    if has_images:
        st.divider()
        st.subheader("10. バナー効果パターン分析 & 次回提案")
        st.caption(
            "高CTRバナーと低CTRバナーの画像を比較し、成功パターンと改善すべきパターンをAIが特定します"
        )

        if st.button("効果パターン分析を実行", key="run_pattern_analysis"):
            with st.spinner("高CTR/低CTRバナーを比較分析中..."):
                try:
                    # CTR上位3 / 下位3を取得
                    df_sorted = df_bp.sort_values("CTR", ascending=False)
                    top3 = df_sorted.head(3)
                    bottom3 = df_sorted.tail(3)

                    # 上位バナー表示
                    st.markdown("**CTR上位3バナー:**")
                    top_cols = st.columns(3)
                    top_paths = []
                    for i, (_, row) in enumerate(top3.iterrows()):
                        b_info = df_banners[df_banners["BANNER_ID"] == row["BANNER_ID"]]
                        if len(b_info) > 0:
                            with top_cols[i]:
                                display_banner_image(b_info.iloc[0]["IMAGE_PATH"],
                                                     caption=f"{row['BANNER_NAME']}\nCTR: {fmt_pct(row['CTR'])}")
                                top_paths.append(b_info.iloc[0]["IMAGE_PATH"])

                    # 下位バナー表示
                    st.markdown("**CTR下位3バナー:**")
                    bot_cols = st.columns(3)
                    bot_paths = []
                    for i, (_, row) in enumerate(bottom3.iterrows()):
                        b_info = df_banners[df_banners["BANNER_ID"] == row["BANNER_ID"]]
                        if len(b_info) > 0:
                            with bot_cols[i]:
                                display_banner_image(b_info.iloc[0]["IMAGE_PATH"],
                                                     caption=f"{row['BANNER_NAME']}\nCTR: {fmt_pct(row['CTR'])}")
                                bot_paths.append(b_info.iloc[0]["IMAGE_PATH"])

                    # 上位1つ vs 下位1つをAI_COMPLETEで詳細比較
                    if top_paths and bot_paths:
                        pattern_sql = f"""
                            SELECT AI_COMPLETE('{LLM_MODEL}',
                                PROMPT('以下は広告バナー2枚です。
画像1は高CTR（クリック率が高い）バナー、画像2は低CTR（クリック率が低い）バナーです。

以下の観点で日本語で詳細に分析してください:
1. 高CTRバナーの成功要因（視覚的要素、構成、メッセージング）
2. 低CTRバナーの改善すべき点
3. 両者の決定的な違い
4. これらの分析から導き出される「効果的なバナーの法則」を5つ
5. 次に制作すべきバナーの具体的な仕様提案（色彩、レイアウト、CTA、コピーの方向性）

画像1（高CTR）: {{0}}
画像2（低CTR）: {{1}}',
                                TO_FILE('{STAGE_NAME}', '{top_paths[0]}'),
                                TO_FILE('{STAGE_NAME}', '{bot_paths[0]}')
                            )) AS PATTERN_ANALYSIS
                        """
                        pattern_result = session.sql(pattern_sql).to_pandas()
                        analysis_text = pattern_result.iloc[0]["PATTERN_ANALYSIS"].replace("\\n", "\n")

                        st.divider()
                        st.markdown("**AI パターン分析結果:**")
                        st.markdown(analysis_text)

                    # テキストベースの全体傾向分析
                    perf_summary = ""
                    for _, row in df_sorted.iterrows():
                        b = df_banners[df_banners["BANNER_ID"] == row["BANNER_ID"]]
                        if len(b) > 0:
                            bi = b.iloc[0]
                            perf_summary += (
                                f"- {row['BANNER_NAME']}: CTR={fmt_pct(row['CTR'])} "
                                f"CVR={fmt_pct(row['CVR'])} "
                                f"スタイル={bi['DESIGN_STYLE']} 訴求={bi['APPEAL_TYPE']} "
                                f"CTA色={bi['CTA_COLOR']} 主色={bi['PRIMARY_COLOR']}\n"
                            )

                    trend_prompt = (
                        "以下は全バナーのパフォーマンスと属性データです（CTR降順）。\n\n"
                        f"{perf_summary}\n"
                        "この全体データから読み取れる傾向を分析し、以下を日本語で回答してください:\n"
                        "1. CTRが高いバナーに共通する属性パターン\n"
                        "2. CVRが高いバナーに共通する属性パターン\n"
                        "3. 属性の組み合わせで最も効果的なパターン\n"
                        "4. 避けるべき属性の組み合わせ\n"
                        "5. 次回キャンペーンで試すべき新しいクリエイティブ方向性3案"
                    ).replace("'", "''")
                    trend_sql = f"SELECT AI_COMPLETE('{LLM_MODEL}', '{trend_prompt}') AS TREND"
                    trend_result = session.sql(trend_sql).to_pandas()

                    st.divider()
                    st.markdown("**全体傾向 & 次回提案:**")
                    st.markdown(trend_result.iloc[0]["TREND"].replace("\\n", "\n"))

                except Exception as e:
                    st.error(f"パターン分析エラー: {e}")


# ==========================================================================
# ページ4: 自然言語クエリ (Cortex Analyst)
# ==========================================================================
def page_nl_query():
    st.header("自然言語クエリ (Cortex Analyst)")
    st.caption(
        "セマンティックビューを通じて、配信データに対して自然言語で問い合わせができます。"
    )

    # セッションステート初期化
    if "analyst_messages" not in st.session_state:
        st.session_state.analyst_messages = []

    # --- プリセット質問 ---
    st.subheader("プリセット質問")
    presets = [
        "CTRが最も高いバナーはどれですか？",
        "キャンペーン別の費用対効果（CPA）を教えてください",
        "モバイルとデスクトップでCVRに差があるバナーは？",
        "地域別のインプレッションとクリック数を表示してください",
        "先月と比較してCTRが下がったバナーはありますか？",
        "訴求タイプ別の平均コンバージョン率は？",
    ]

    preset_cols = st.columns(3)
    for i, q in enumerate(presets):
        col = preset_cols[i % 3]
        if col.button(q, key=f"preset_{i}", use_container_width=True):
            _run_analyst_query(q)

    # --- 自由入力 ---
    st.subheader("自由入力")
    with st.form("analyst_form", clear_on_submit=True):
        user_query = st.text_input("自然言語で質問を入力してください")
        submitted = st.form_submit_button("送信")

    if submitted and user_query:
        _run_analyst_query(user_query)

    # --- 会話履歴 ---
    if st.session_state.analyst_messages:
        st.divider()
        st.subheader("会話履歴")
        for msg in st.session_state.analyst_messages:
            if msg["role"] == "user":
                st.markdown(f"**Q:** {msg['text']}")
            else:
                _display_analyst_result(msg["result"])
            st.markdown("---")

    # クリアボタン
    if st.session_state.analyst_messages:
        if st.button("会話をクリア"):
            st.session_state.analyst_messages = []
            safe_rerun()


def _run_analyst_query(question):
    st.session_state.analyst_messages.append({"role": "user", "text": question})

    payload = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": question}]}
        ],
        "semantic_view": SEMANTIC_VIEW,
    }

    try:
        resp = _snowflake.send_snow_api_request(
            "POST", ANALYST_ENDPOINT, {}, {}, payload, None, ANALYST_TIMEOUT_MS
        )

        status = resp.get("status", -1)
        content_raw = resp.get("content", "")

        result = {
            "status": status,
            "text_response": None,
            "sql_statement": None,
            "suggestions": None,
            "error": None,
            "query_result": None,
            "user_question": question,
        }

        if status != 200:
            result["error"] = f"HTTP {status}: {content_raw[:500]}"
        else:
            body = json.loads(content_raw) if isinstance(content_raw, str) else content_raw
            message = body.get("message", {})
            contents = message.get("content", [])

            for item in contents:
                item_type = item.get("type", "")
                if item_type == "text":
                    result["text_response"] = item.get("text", "")
                elif item_type == "sql":
                    result["sql_statement"] = item.get("statement", "")
                elif item_type == "suggestions":
                    result["suggestions"] = item.get("suggestions", [])

            if result["sql_statement"]:
                try:
                    df = session.sql(result["sql_statement"]).to_pandas()
                    result["query_result"] = df
                except Exception as e:
                    result["query_result"] = f"SQL実行エラー: {e}"

        st.session_state.analyst_messages.append({"role": "assistant", "result": result})
    except Exception as e:
        st.session_state.analyst_messages.append({
            "role": "assistant",
            "result": {"error": f"API呼び出しエラー: {e}", "text_response": None,
                       "sql_statement": None, "suggestions": None, "query_result": None,
                       "user_question": question}
        })

    safe_rerun()


def _display_analyst_result(result):
    if result.get("error"):
        st.error(result["error"])

    if result.get("text_response"):
        st.markdown(result["text_response"])

    if result.get("sql_statement"):
        with st.expander("生成されたSQL", expanded=False):
            st.code(result["sql_statement"], language="sql")

    if result.get("query_result") is not None:
        qr = result["query_result"]
        if isinstance(qr, str):
            st.warning(qr)
        else:
            st.dataframe(qr, use_container_width=True)

            # --- AI日本語サマリ生成 ---
            user_q = result.get("user_question", "")
            try:
                # DataFrameの先頭行をテキスト化（最大20行）
                preview = qr.head(20).to_string(index=False)
                cols_info = ", ".join([f"{c}({qr[c].dtype})" for c in qr.columns])
                summary_prompt = (
                    f"以下はSnowflakeのクエリ結果です。ユーザーの質問「{user_q}」に対する回答として、"
                    f"データの要点を日本語で簡潔にまとめてください（3〜5文程度）。"
                    f"数値はそのまま引用し、傾向・比較・示唆を含めてください。\n\n"
                    f"カラム: {cols_info}\n"
                    f"行数: {len(qr)}\n\n"
                    f"データ:\n{preview}"
                ).replace("'", "''")
                summary_sql = f"""
                    SELECT AI_COMPLETE('{LLM_MODEL}', '{summary_prompt}') AS SUMMARY
                """
                summary_result = session.sql(summary_sql).to_pandas()
                summary_text = summary_result.iloc[0]["SUMMARY"].replace("\\n", "\n")
                st.markdown("---")
                st.markdown("**分析サマリ:**")
                st.info(summary_text)
            except Exception:
                pass  # サマリ生成失敗は無視

            # 数値列が2列以上あれば自動チャート提案
            num_cols = qr.select_dtypes(include=["number"]).columns.tolist()
            non_num_cols = [c for c in qr.columns if c not in num_cols]
            if len(num_cols) >= 1 and len(non_num_cols) >= 1:
                with st.expander("チャート表示"):
                    x_col = non_num_cols[0]
                    y_col = num_cols[0]
                    chart = (
                        alt.Chart(qr)
                        .mark_bar()
                        .encode(
                            x=alt.X(f"{x_col}:N"),
                            y=alt.Y(f"{y_col}:Q"),
                            tooltip=list(qr.columns),
                        )
                    )
                    st.altair_chart(chart, use_container_width=True)

    if result.get("suggestions"):
        with st.expander("関連する質問候補"):
            for s in result["suggestions"]:
                st.markdown(f"- {s}")


# ==========================================================================
# ページ5: 改善提案AIアドバイザー
# ==========================================================================
def page_advisor():
    st.header("改善提案AIアドバイザー")
    st.caption(
        "配信データとバナー属性を総合的に分析し、AI_COMPLETEで改善提案を生成します。"
    )

    df_campaigns = load_campaigns()
    df_banners = load_banners()
    df_bp = load_banner_performance()

    # --- キャンペーン選択 ---
    selected_campaign = st.selectbox(
        "分析対象キャンペーン",
        df_campaigns["CAMPAIGN_ID"].tolist(),
        format_func=lambda cid: df_campaigns[df_campaigns["CAMPAIGN_ID"] == cid]["CAMPAIGN_NAME"].values[0]
    )

    campaign_info = df_campaigns[df_campaigns["CAMPAIGN_ID"] == selected_campaign].iloc[0]
    campaign_banners = df_banners[df_banners["CAMPAIGN_ID"] == selected_campaign]
    campaign_perf = df_bp[df_bp["CAMPAIGN_NAME"] == campaign_info["CAMPAIGN_NAME"]]

    # キャンペーン概要表示
    st.markdown(f"""
    **キャンペーン情報:**
    - 広告主: {campaign_info['ADVERTISER']} ({campaign_info['INDUSTRY']})
    - 目的: {campaign_info['OBJECTIVE']}
    - 期間: {campaign_info['START_DATE']} 〜 {campaign_info['END_DATE']}
    - 予算: ¥{fmt_num(campaign_info['BUDGET_JPY'])}
    - バナー数: {len(campaign_banners)}
    """)

    # バナー別パフォーマンス表示（画像付き）
    if len(campaign_perf) > 0:
        st.markdown("**バナー別パフォーマンス:**")
        # 2列ずつ表示
        banner_list = list(campaign_perf.iterrows())
        for i in range(0, len(banner_list), 2):
            cols = st.columns(2)
            for j, col in enumerate(cols):
                idx = i + j
                if idx < len(banner_list):
                    _, row = banner_list[idx]
                    with col:
                        # 画像パスを取得
                        b_info = campaign_banners[campaign_banners["BANNER_ID"] == row["BANNER_ID"]]
                        if len(b_info) > 0:
                            display_banner_image(
                                b_info.iloc[0]["IMAGE_PATH"],
                                caption=row["BANNER_NAME"],
                                width=300
                            )
                        st.markdown(
                            f"**{row['BANNER_NAME']}** [{row['BANNER_SIZE']}]\n\n"
                            f"CTR: {fmt_pct(row['CTR'])} | CVR: {fmt_pct(row['CVR'])} | "
                            f"CPA: ¥{fmt_num(row.get('CPA_JPY', 0))}"
                        )

    # --- 自由記述の質問入力（RAG） ---
    st.divider()
    advisor_question = st.text_area(
        "質問・相談したいこと（自由記述）",
        placeholder="例: CTAの色を暖色系に変えたらCVRは上がりますか？モバイル向けの最適なレイアウトは？予算配分をどう見直すべき？",
        height=100,
        key="advisor_question"
    )

    # --- プロンプト構築 ---
    prompt_context = _build_advisor_context(campaign_info, campaign_banners, campaign_perf)

    # --- AI提案生成 ---
    if st.button("AI提案を生成", type="primary"):
        if not advisor_question.strip():
            st.warning("質問を入力してください。")
            st.stop()

        # ユーザーの質問文で関連ナレッジを動的検索（RAG）
        knowledge_count = 0
        with st.spinner("過去ナレッジを検索中..."):
            knowledge_results = search_knowledge_base(advisor_question.strip(), limit=5)
            knowledge_count = len(knowledge_results)
            if knowledge_results:
                knowledge_text = format_knowledge_context(knowledge_results)
                st.info(f"{knowledge_count}件の過去ナレッジを参照して提案を強化します")
                with st.expander("参照ナレッジ一覧", expanded=False):
                    for r in knowledge_results:
                        st.caption(f"- [{r.get('DOC_TITLE', '')}] {r.get('CHUNK_TEXT', '')[:80]}...")
            else:
                knowledge_text = ""
                st.caption("関連する過去ナレッジが見つかりませんでした。一般的な知識で提案します。")

        prompt = f"""あなたは広告バナー最適化の専門アナリストです。
以下のキャンペーンデータと過去の社内ナレッジを参考に、ユーザーの質問に具体的に回答してください。

【ユーザーの質問】
{advisor_question}

【現在のキャンペーンデータ】
{prompt_context}

【過去の社内分析ナレッジ（類似業界・類似施策の実績データ）】
{knowledge_text if knowledge_text else "（該当する過去ナレッジなし）"}

以下の構成で回答してください:
1. 質問に対する直接的な回答
2. 現在のキャンペーンデータからの分析根拠
3. 過去ナレッジからの示唆（該当する事例や数値を引用）
4. 具体的な改善提案とアクションプラン
5. 定量的な改善見込み（過去実績に基づく期待値）

過去ナレッジの具体的な数値や事例を引用しながら、根拠のある提案をしてください。
"""

        with st.spinner("AI_COMPLETEで提案を生成中..."):
            try:
                escaped_prompt = prompt.replace("'", "''")
                sql = f"""
                    SELECT AI_COMPLETE(
                        '{LLM_MODEL}',
                        '{escaped_prompt}'
                    ) AS ADVICE
                """
                result = session.sql(sql).to_pandas()
                advice = result.iloc[0]["ADVICE"].replace("\\n", "\n")

                st.divider()
                st.subheader("AI提案")
                if knowledge_count > 0:
                    st.caption(f"(過去ナレッジ{knowledge_count}件を参照)")
                st.markdown(advice)

                # セッションに保存
                if "advisor_history" not in st.session_state:
                    st.session_state.advisor_history = []
                st.session_state.advisor_history.append({
                    "campaign": campaign_info["CAMPAIGN_NAME"],
                    "question": advisor_question,
                    "advice": advice,
                    "knowledge_used": knowledge_count,
                })
            except Exception as e:
                st.error(f"AI_COMPLETE実行エラー: {e}")

    # --- 過去の提案履歴 ---
    if st.session_state.get("advisor_history"):
        st.divider()
        st.subheader("過去の提案履歴")
        for i, h in enumerate(reversed(st.session_state.advisor_history)):
            q_label = h.get("question", h.get("type", ""))[:40]
            kb_label = f" +ナレッジ{h.get('knowledge_used', 0)}件" if h.get("knowledge_used") else ""
            with st.expander(f"[{h['campaign']}] {q_label}...{kb_label}", expanded=False):
                st.markdown(h["advice"])


def _build_advisor_context(campaign_info, campaign_banners, campaign_perf):
    """AIアドバイザー用のコンテキスト文字列を構築"""
    lines = []
    lines.append(f"キャンペーン名: {campaign_info['CAMPAIGN_NAME']}")
    lines.append(f"広告主: {campaign_info['ADVERTISER']} ({campaign_info['INDUSTRY']})")
    lines.append(f"目的: {campaign_info['OBJECTIVE']}")
    lines.append(f"ターゲット: {campaign_info.get('TARGET_AUDIENCE', 'N/A')}")
    lines.append(f"予算: ¥{fmt_num(campaign_info['BUDGET_JPY'])}")
    lines.append(f"期間: {campaign_info['START_DATE']} ~ {campaign_info['END_DATE']}")
    lines.append("")

    lines.append("=== バナー別パフォーマンス ===")
    for _, row in campaign_perf.iterrows():
        banner_info = campaign_banners[campaign_banners["BANNER_ID"] == row["BANNER_ID"]]
        if len(banner_info) > 0:
            bi = banner_info.iloc[0]
            lines.append(
                f"- {row['BANNER_NAME']} [{row['BANNER_SIZE']}]"
                f"  スタイル:{bi['DESIGN_STYLE']} 訴求:{bi['APPEAL_TYPE']}"
                f"  見出し:「{bi['HEADLINE_TEXT']}」 CTA:「{bi['CTA_TEXT']}」 CTA色:{bi['CTA_COLOR']}"
                f"  imp:{fmt_num(row['IMPRESSIONS'])} click:{fmt_num(row['CLICKS'])}"
                f"  CV:{fmt_num(row['CONVERSIONS'])} CTR:{fmt_pct(row['CTR'])}"
                f"  CVR:{fmt_pct(row['CVR'])} CPA:¥{fmt_num(row.get('CPA_JPY', 0))}"
                f"  費用:¥{fmt_num(row['COST_JPY'])}"
            )

    # デバイス別サマリー
    try:
        dev_data = run_query(f"""
            SELECT dr.DEVICE_TYPE,
                   SUM(dr.IMPRESSIONS) AS IMP, SUM(dr.CLICKS) AS CLK, SUM(dr.CONVERSIONS) AS CV,
                   CASE WHEN SUM(dr.IMPRESSIONS)>0 THEN SUM(dr.CLICKS)::FLOAT/SUM(dr.IMPRESSIONS) ELSE 0 END AS CTR
            FROM {DB_SCHEMA}.DELIVERY_RESULTS dr
            JOIN {DB_SCHEMA}.AD_BANNERS b ON dr.BANNER_ID = b.BANNER_ID
            WHERE b.CAMPAIGN_ID = {campaign_info['CAMPAIGN_ID']}
            GROUP BY dr.DEVICE_TYPE ORDER BY IMP DESC
        """)
        lines.append("")
        lines.append("=== デバイス別サマリー ===")
        for _, d in dev_data.iterrows():
            lines.append(f"- {d['DEVICE_TYPE']}: imp={fmt_num(d['IMP'])} CTR={fmt_pct(d['CTR'])}")
    except Exception:
        pass

    # 地域別サマリー
    try:
        reg_data = run_query(f"""
            SELECT dr.REGION,
                   SUM(dr.IMPRESSIONS) AS IMP, SUM(dr.CLICKS) AS CLK, SUM(dr.CONVERSIONS) AS CV,
                   CASE WHEN SUM(dr.IMPRESSIONS)>0 THEN SUM(dr.CLICKS)::FLOAT/SUM(dr.IMPRESSIONS) ELSE 0 END AS CTR
            FROM {DB_SCHEMA}.DELIVERY_RESULTS dr
            JOIN {DB_SCHEMA}.AD_BANNERS b ON dr.BANNER_ID = b.BANNER_ID
            WHERE b.CAMPAIGN_ID = {campaign_info['CAMPAIGN_ID']}
            GROUP BY dr.REGION ORDER BY IMP DESC
        """)
        lines.append("")
        lines.append("=== 地域別サマリー ===")
        for _, r in reg_data.iterrows():
            lines.append(f"- {r['REGION']}: imp={fmt_num(r['IMP'])} CTR={fmt_pct(r['CTR'])}")
    except Exception:
        pass

    return "\n".join(lines)


# ==========================================================================
# ページ6: ナレッジベースRAG分析
# ==========================================================================
def page_knowledge_rag():
    st.header("ナレッジベースRAG分析")
    st.caption(
        "過去の社内分析レポートをCortex Searchで検索し、"
        "現在のキャンペーンデータと組み合わせてAI分析を行います。"
    )

    tab1, tab2 = st.tabs(["ナレッジ検索", "ナレッジ強化アドバイザー"])

    # --- タブ1: ナレッジ検索 ---
    with tab1:
        st.subheader("過去ナレッジ検索")
        st.caption("フリーテキストで過去の分析レポートから関連知見を検索します。")

        search_query = st.text_input(
            "検索キーワード",
            placeholder="例: CTAボタンの色とコンバージョン率の関係",
            key="knowledge_search_query"
        )
        search_limit = st.slider("検索結果数", min_value=1, max_value=10, value=5, key="knowledge_search_limit")

        if st.button("検索", key="btn_knowledge_search"):
            if not search_query.strip():
                st.warning("検索キーワードを入力してください。")
            else:
                with st.spinner("Cortex Searchでナレッジを検索中..."):
                    results = search_knowledge_base(search_query, limit=search_limit)

                if results:
                    st.success(f"{len(results)}件のナレッジが見つかりました")
                    for i, r in enumerate(results, 1):
                        title = r.get("DOC_TITLE", "不明")
                        filename = r.get("DOC_FILENAME", "")
                        text = r.get("CHUNK_TEXT", "")
                        scores = r.get("@scores", {})
                        similarity = scores.get("cosine_similarity", 0)

                        with st.expander(f"#{i} [{title}] (類似度: {similarity:.3f})", expanded=(i <= 2)):
                            st.caption(f"ファイル: {filename}")
                            st.markdown(text.replace("\\n", "\n"))
                else:
                    st.info("該当するナレッジが見つかりませんでした。")

    # --- タブ2: ナレッジ強化アドバイザー ---
    with tab2:
        st.subheader("ナレッジ強化アドバイザー")
        st.caption(
            "現在のキャンペーンデータと過去の社内ナレッジを組み合わせて、"
            "会社独自の経験に基づいた改善提案を生成します。"
        )

        df_campaigns = load_campaigns()
        df_banners = load_banners()
        df_bp = load_banner_performance()

        # キャンペーン選択
        selected_campaign = st.selectbox(
            "分析対象キャンペーン",
            df_campaigns["CAMPAIGN_ID"].tolist(),
            format_func=lambda cid: df_campaigns[df_campaigns["CAMPAIGN_ID"] == cid]["CAMPAIGN_NAME"].values[0],
            key="rag_campaign_selector"
        )

        campaign_info = df_campaigns[df_campaigns["CAMPAIGN_ID"] == selected_campaign].iloc[0]
        campaign_banners = df_banners[df_banners["CAMPAIGN_ID"] == selected_campaign]
        campaign_perf = df_bp[df_bp["CAMPAIGN_NAME"] == campaign_info["CAMPAIGN_NAME"]]

        # キャンペーン概要表示
        st.markdown(f"""
**キャンペーン情報:**
- 広告主: {campaign_info['ADVERTISER']} ({campaign_info['INDUSTRY']})
- 目的: {campaign_info['OBJECTIVE']}
- 期間: {campaign_info['START_DATE']} 〜 {campaign_info['END_DATE']}
- バナー数: {len(campaign_banners)}
        """)

        # 自由記述の質問入力
        user_question = st.text_area(
            "質問・分析したいこと（自由記述）",
            placeholder="例: このキャンペーンのCTAボタンの色をオレンジに変えたらCVRは改善しますか？過去に似た施策はありますか？",
            height=100,
            key="rag_user_question"
        )

        if st.button("ナレッジ強化AI分析を実行", type="primary", key="btn_rag_analysis"):
            if not user_question.strip():
                st.warning("質問を入力してください。")
                st.stop()
            search_q = user_question.strip()

            with st.spinner("Step 1/3: 関連ナレッジを検索中..."):
                knowledge_results = search_knowledge_base(search_q, limit=5)
                knowledge_context = format_knowledge_context(knowledge_results)

            if knowledge_results:
                st.info(f"{len(knowledge_results)}件の過去ナレッジを参照します")
                with st.expander("参照ナレッジ一覧", expanded=False):
                    for r in knowledge_results:
                        st.caption(f"- [{r.get('DOC_TITLE', '')}] {r.get('CHUNK_TEXT', '')[:80]}...")

            with st.spinner("Step 2/3: キャンペーンデータを収集中..."):
                campaign_context = _build_advisor_context(campaign_info, campaign_banners, campaign_perf)

            with st.spinner("Step 3/3: AI_COMPLETEでナレッジ強化分析を生成中..."):
                rag_prompt = f"""あなたは広告バナー最適化の専門アナリストです。
以下の2つの情報源を組み合わせて、ユーザーの質問に具体的に回答してください。

【ユーザーの質問】
{user_question}

【情報源1: 現在のキャンペーンデータ】
{campaign_context}

【情報源2: 過去の社内分析ナレッジ（類似業界・類似施策の実績データ）】
{knowledge_context if knowledge_context else "（該当する過去ナレッジなし）"}

以下の構成で回答してください:
1. 現状分析（現在のキャンペーンデータの評価）
2. 過去ナレッジからの示唆（類似事例との比較・適用可能な知見）
3. 具体的な改善提案（過去の成功パターンを現キャンペーンに適用）
4. 定量的な改善見込み（過去実績に基づく期待値）
5. 実施優先順位とアクションプラン

過去ナレッジの具体的な数値や事例を引用しながら、根拠のある提案をしてください。
"""

                try:
                    escaped = rag_prompt.replace("'", "''")
                    sql = f"SELECT AI_COMPLETE('{LLM_MODEL}', '{escaped}') AS ADVICE"
                    result = session.sql(sql).to_pandas()
                    advice = result.iloc[0]["ADVICE"].replace("\\n", "\n")

                    st.divider()
                    st.subheader("ナレッジ強化AI分析結果")
                    st.markdown(advice)

                    # セッションに保存
                    if "rag_history" not in st.session_state:
                        st.session_state.rag_history = []
                    st.session_state.rag_history.append({
                        "campaign": campaign_info["CAMPAIGN_NAME"],
                        "question": user_question,
                        "advice": advice,
                        "knowledge_count": len(knowledge_results),
                    })
                except Exception as e:
                    st.error(f"AI_COMPLETE実行エラー: {e}")

        # 過去の分析履歴
        if st.session_state.get("rag_history"):
            st.divider()
            st.subheader("過去のナレッジ強化分析履歴")
            for i, h in enumerate(reversed(st.session_state.rag_history)):
                with st.expander(
                    f"[{h['campaign']}] {h.get('question', h.get('theme', ''))[:40]}... (参照ナレッジ: {h['knowledge_count']}件)",
                    expanded=False
                ):
                    st.markdown(h["advice"])


# ==========================================================================
# メインルーティング
# ==========================================================================
st.title("広告バナー分析ダッシュボード")
st.caption("Snowflake Cortex AI SQL関数を活用した多角的広告分析")

with st.sidebar:
    st.header("ナビゲーション")
    page = st.radio(
        "ページ選択",
        [
            "ダッシュボード概要",
            "A/Bテスト分析",
            "AI画像分析",
            "自然言語クエリ",
            "改善提案AIアドバイザー",
            "ナレッジベースRAG分析",
        ],
        index=0,
    )

    st.divider()
    st.caption(f"DB: {DB_SCHEMA}")
    st.caption(f"LLM: {LLM_MODEL}")

if page == "ダッシュボード概要":
    page_dashboard()
elif page == "A/Bテスト分析":
    page_ab_test()
elif page == "AI画像分析":
    page_ai_image()
elif page == "自然言語クエリ":
    page_nl_query()
elif page == "改善提案AIアドバイザー":
    page_advisor()
elif page == "ナレッジベースRAG分析":
    page_knowledge_rag()

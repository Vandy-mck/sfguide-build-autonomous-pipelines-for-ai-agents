"""
EuroShip Logistics — TMS Operations Dashboard
Streamlit in Snowflake app for monitoring the Transportation Management System.

Connects to SUMMIT_DB_DEV.ANALYTICS views:
  - ORDER_SUMMARY
  - PACKAGE_TRACKING
  - FRAUD_DETECTION
  - LOCATION_ACTIVITY
"""

import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="EuroShip Logistics — TMS Operations",
    page_icon="📦",
    layout="wide",
)

# --- Snowflake Connection ---
session = get_active_session()

DB_SCHEMA = "SUMMIT_DB_DEV.ANALYTICS"


@st.cache_data(ttl=60)
def run_query(sql: str) -> pd.DataFrame:
    return session.sql(sql).to_pandas()


# =============================================================================
# PAGE: Dashboard
# =============================================================================
def page_dashboard():
    st.title("EuroShip Logistics — TMS Operations")
    st.caption("Real-time overview of orders, packages, and fraud alerts")

    # Date range selector
    date_range = run_query(f"""
        SELECT MIN(DATE(ORDER_DATE)) AS MIN_DATE, MAX(DATE(ORDER_DATE)) AS MAX_DATE
        FROM {DB_SCHEMA}.ORDER_SUMMARY
    """)

    if date_range.empty or date_range["MIN_DATE"].iloc[0] is None:
        st.info("No order data available yet.")
        return

    min_date = pd.to_datetime(date_range["MIN_DATE"].iloc[0]).date()
    max_date = pd.to_datetime(date_range["MAX_DATE"].iloc[0]).date()

    col_start, col_end = st.columns(2)
    with col_start:
        start_date = st.date_input("From", value=min_date, min_value=min_date, max_value=max_date)
    with col_end:
        end_date = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date)

    date_filter = f"DATE(ORDER_DATE) BETWEEN '{start_date}' AND '{end_date}'"

    # KPIs
    kpi_sql = f"""
    SELECT
        (SELECT COUNT(*) FROM {DB_SCHEMA}.ORDER_SUMMARY WHERE {date_filter}) AS total_orders,
        (SELECT COUNT(*) FROM {DB_SCHEMA}.PACKAGE_TRACKING WHERE PACKAGE_STATUS = 'IN_TRANSIT') AS packages_in_transit,
        (SELECT COUNT(*) FROM {DB_SCHEMA}.FRAUD_DETECTION WHERE IS_FRAUD = TRUE AND PAYMENT_TIMESTAMP BETWEEN '{start_date}' AND '{end_date}') AS fraud_alerts,
        (SELECT ROUND(AVG(TRANSIT_HOURS), 1) FROM {DB_SCHEMA}.PACKAGE_TRACKING WHERE TRANSIT_HOURS IS NOT NULL) AS avg_transit_hours
    """
    kpis = run_query(kpi_sql)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Orders", f"{kpis['TOTAL_ORDERS'].iloc[0]:,}")
    col2.metric("Packages In Transit", f"{kpis['PACKAGES_IN_TRANSIT'].iloc[0]:,}")
    col3.metric("Fraud Alerts", f"{kpis['FRAUD_ALERTS'].iloc[0]:,}")
    col4.metric("Avg Transit Hours", f"{kpis['AVG_TRANSIT_HOURS'].iloc[0]}")

    st.divider()

    # Orders over time
    st.subheader("Orders Over Time")
    orders_time = run_query(f"""
        SELECT DATE(ORDER_DATE) AS ORDER_DAY, COUNT(*) AS NUM_ORDERS
        FROM {DB_SCHEMA}.ORDER_SUMMARY
        WHERE {date_filter}
        GROUP BY ORDER_DAY
        ORDER BY ORDER_DAY
    """)
    if not orders_time.empty:
        st.line_chart(orders_time, x="ORDER_DAY", y="NUM_ORDERS")
    else:
        st.info("No order data available for the selected date range.")

    # Two columns for charts
    left, right = st.columns(2)

    with left:
        st.subheader("Package Status Distribution")
        status_dist = run_query(f"""
            SELECT PACKAGE_STATUS, COUNT(*) AS COUNT
            FROM {DB_SCHEMA}.PACKAGE_TRACKING
            GROUP BY PACKAGE_STATUS
            ORDER BY COUNT DESC
        """)
        if not status_dist.empty:
            st.bar_chart(status_dist, x="PACKAGE_STATUS", y="COUNT")

    with right:
        st.subheader("Top 10 Busiest Hubs")
        top_hubs = run_query(f"""
            SELECT LOCATION_NAME, SUM(NUM_PACKAGES) AS TOTAL_PACKAGES
            FROM {DB_SCHEMA}.LOCATION_ACTIVITY
            WHERE ACTIVITY_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY LOCATION_NAME
            ORDER BY TOTAL_PACKAGES DESC
            LIMIT 10
        """)
        if not top_hubs.empty:
            chart = (
                alt.Chart(top_hubs)
                .mark_bar()
                .encode(
                    x=alt.X("TOTAL_PACKAGES:Q", title="Packages Processed"),
                    y=alt.Y("LOCATION_NAME:N", sort="-x", title="Location"),
                )
                .properties(height=350)
            )
            st.altair_chart(chart, use_container_width=True)


# =============================================================================
# PAGE: Package Tracking
# =============================================================================
def page_package_tracking():
    st.title("Package Tracking")
    st.caption("Search and filter packages across the logistics network")

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        statuses = run_query(f"""
            SELECT DISTINCT PACKAGE_STATUS
            FROM {DB_SCHEMA}.PACKAGE_TRACKING
            ORDER BY PACKAGE_STATUS
        """)
        status_options = ["All"] + statuses["PACKAGE_STATUS"].tolist()
        selected_status = st.selectbox("Package Status", status_options)

    with col2:
        carriers = run_query(f"""
            SELECT DISTINCT CARRIER
            FROM {DB_SCHEMA}.PACKAGE_TRACKING
            WHERE CARRIER IS NOT NULL
            ORDER BY CARRIER
        """)
        carrier_options = ["All"] + carriers["CARRIER"].tolist()
        selected_carrier = st.selectbox("Carrier", carrier_options)

    # Build query
    where_clauses = []
    if selected_status != "All":
        where_clauses.append(f"PACKAGE_STATUS = '{selected_status}'")
    if selected_carrier != "All":
        where_clauses.append(f"CARRIER = '{selected_carrier}'")

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    packages = run_query(f"""
        SELECT
            TRACKING_NUMBER,
            CUSTOMER_NAME,
            DESTINATION_CITY,
            DESTINATION_COUNTRY,
            PACKAGE_STATUS,
            CARRIER,
            HUBS_VISITED,
            TRANSIT_HOURS
        FROM {DB_SCHEMA}.PACKAGE_TRACKING
        {where_sql}
        ORDER BY TRANSIT_HOURS DESC NULLS LAST
        LIMIT 500
    """)

    st.dataframe(packages, use_container_width=True)
    st.caption(f"Showing {len(packages)} packages")


# =============================================================================
# PAGE: Fraud Alerts
# =============================================================================
def page_fraud_alerts():
    st.title("Fraud Alerts")
    st.caption("AI-powered payment fraud detection results")

    min_score = st.slider(
        "Minimum Fraud Score",
        min_value=0.30,
        max_value=1.0,
        value=0.30,
        step=0.05,
    )

    fraud = run_query(f"""
        SELECT
            PAYMENT_ID,
            CUSTOMER_NAME,
            CUSTOMER_COUNTRY,
            PAYMENT_METHOD,
            CARD_BRAND,
            PAYMENT_AMOUNT,
            FRAUD_SCORE,
            FRAUD_TYPE,
            FRAUD_SIGNALS,
            EXPLANATION,
            PAYMENT_TIMESTAMP
        FROM {DB_SCHEMA}.FRAUD_DETECTION
        WHERE IS_FRAUD = TRUE AND FRAUD_SCORE >= {min_score}
        ORDER BY FRAUD_SCORE DESC, PAYMENT_TIMESTAMP DESC
        LIMIT 200
    """)

    if fraud.empty:
        st.info("No fraudulent payments found above the selected threshold.")
    else:
        st.metric("Flagged Payments", f"{len(fraud)}")
        st.dataframe(fraud, use_container_width=True)


# =============================================================================
# PAGE: Location Performance
# =============================================================================
def page_location_performance():
    st.title("Location Performance")
    st.caption("Hub and warehouse throughput and processing times")

    # Date filter
    date_range = run_query(f"""
        SELECT MIN(ACTIVITY_DATE) AS MIN_DATE, MAX(ACTIVITY_DATE) AS MAX_DATE
        FROM {DB_SCHEMA}.LOCATION_ACTIVITY
    """)

    if date_range.empty or date_range["MIN_DATE"].iloc[0] is None:
        st.info("No location activity data available yet.")
        return

    min_date = pd.to_datetime(date_range["MIN_DATE"].iloc[0]).date()
    max_date = pd.to_datetime(date_range["MAX_DATE"].iloc[0]).date()

    selected_date = st.date_input(
        "Activity Date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
    )

    locations = run_query(f"""
        SELECT
            LOCATION_NAME,
            LOCATION_TYPE,
            CITY,
            COUNTRY,
            NUM_PACKAGES,
            PROCESSING_P90_MINUTES
        FROM {DB_SCHEMA}.LOCATION_ACTIVITY
        WHERE ACTIVITY_DATE = '{selected_date}'
        ORDER BY NUM_PACKAGES DESC
    """)

    if locations.empty:
        st.info(f"No activity data for {selected_date}.")
        return

    st.dataframe(locations, use_container_width=True)

    st.subheader("P90 Processing Time by Location")
    chart = (
        alt.Chart(locations)
        .mark_bar()
        .encode(
            x=alt.X("PROCESSING_P90_MINUTES:Q", title="P90 Processing (minutes)"),
            y=alt.Y("LOCATION_NAME:N", sort="-x", title="Location"),
            color=alt.Color("LOCATION_TYPE:N", title="Type"),
        )
        .properties(height=400)
    )
    st.altair_chart(chart, use_container_width=True)


# =============================================================================
# NAVIGATION
# =============================================================================
page = st.sidebar.radio(
    "Navigate",
    ["Dashboard", "Package Tracking", "Fraud Alerts", "Location Performance"],
)

if page == "Dashboard":
    page_dashboard()
elif page == "Package Tracking":
    page_package_tracking()
elif page == "Fraud Alerts":
    page_fraud_alerts()
elif page == "Location Performance":
    page_location_performance()

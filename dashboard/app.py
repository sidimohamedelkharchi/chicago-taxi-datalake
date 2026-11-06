"""BI exposition layer: Streamlit dashboard + free SQL on the gold tables (DuckDB)."""

import streamlit as st

from dashboard.gold_store import connect

st.set_page_config(page_title="Chicago Taxi - Gold KPIs", layout="wide")
st.title("Chicago Taxi Trips — Gold layer")


@st.cache_resource(ttl=300)
def get_connection():
    return connect()


con, tables = get_connection()
if st.sidebar.button("Reload gold tables"):
    get_connection.clear()
    st.rerun()

if not tables:
    st.warning("No gold table yet. Run the pipeline (`make run` or *Materialize all* in Dagster), then reload.")
    st.stop()

tab_overview, tab_zones, tab_demand, tab_sql = st.tabs(["Overview", "Pickup zones", "Demand", "SQL"])

with tab_overview:
    totals = (
        con.sql(
            """SELECT sum(trips) AS trips, sum(revenue_total) AS revenue,
                  sum(avg_duration_min * trips) / sum(trips) AS avg_duration,
                  min(trip_date) AS first_day, max(trip_date) AS last_day
           FROM daily_kpis"""
        )
        .df()
        .iloc[0]
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Trips", f"{int(totals.trips):,}")
    c2.metric("Revenue", f"${totals.revenue:,.0f}")
    c3.metric("Avg duration", f"{totals.avg_duration:.1f} min")
    c4.metric("Period", f"{totals.first_day:%Y-%m-%d} → {totals.last_day:%Y-%m-%d}")

    daily = con.sql("SELECT * FROM daily_kpis ORDER BY trip_date").df().set_index("trip_date")
    st.subheader("Revenue per day ($)")
    st.line_chart(daily[["revenue_total"]])
    st.subheader("Average and median trip duration (min)")
    st.line_chart(daily[["avg_duration_min", "median_duration_min"]])
    st.subheader("Payment mix")
    st.dataframe(con.sql("SELECT * FROM payment_mix ORDER BY trips DESC").df(), hide_index=True)

with tab_zones:
    periods = [r[0] for r in con.sql("SELECT DISTINCT period FROM pickup_zones ORDER BY period").fetchall()]
    period = st.selectbox("Period", periods, index=periods.index("ALL") if "ALL" in periods else 0)
    top_n = st.slider("Top N", 5, 30, 10)
    zones = con.execute(
        "SELECT rank, pickup_zone, side, trips, trip_share, revenue_total, avg_trip_total, avg_tip_rate "
        "FROM pickup_zones WHERE period = ? AND rank <= ? ORDER BY rank",
        [period, top_n],
    ).df()
    st.bar_chart(zones.set_index("pickup_zone")[["trips"]], horizontal=True)
    st.dataframe(zones, hide_index=True)

with tab_demand:
    heat = con.sql("SELECT weekday, weekday_name, pickup_hour, avg_trips_per_day FROM hourly_demand").df()
    st.subheader("Average trips per day — weekday × pickup hour")
    st.vega_lite_chart(
        heat,
        {
            "mark": "rect",
            "encoding": {
                "x": {"field": "pickup_hour", "type": "ordinal", "title": "Pickup hour"},
                "y": {
                    "field": "weekday_name",
                    "type": "ordinal",
                    "title": None,
                    "sort": {"field": "weekday", "op": "min"},
                },
                "color": {"field": "avg_trips_per_day", "type": "quantitative", "title": "Trips/day"},
                "tooltip": [{"field": "weekday_name"}, {"field": "pickup_hour"}, {"field": "avg_trips_per_day"}],
            },
        },
        width="stretch",
    )

with tab_sql:
    st.caption(f"Tables: {', '.join(tables)} — DuckDB SQL dialect")
    query = st.text_area("SQL", "SELECT * FROM pickup_zones WHERE period = 'ALL' ORDER BY rank LIMIT 10", height=120)
    if st.button("Run", key="run_sql"):
        try:
            st.dataframe(con.sql(query).df(), hide_index=True)
        except Exception as exc:
            st.error(str(exc))

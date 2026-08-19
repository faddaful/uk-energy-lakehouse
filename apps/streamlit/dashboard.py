"""Personal energy dashboard. Reads gold straight out of the DuckDB file
dbt already built, no export step, because the app runs on the same box
as the pipeline and reaches your phone over Tailscale from there, not the
public internet. If that ever changes (e.g. a public Streamlit Cloud
deployment), that's the point to add an export asset; it isn't needed yet.

Run with:
    uv run streamlit run apps/streamlit/dashboard.py
"""

import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[2]
DB_NAME = "lakehouse_azure.duckdb" if os.environ.get("TARGET") == "azure" else "lakehouse.duckdb"
DB_PATH = REPO_ROOT / "data" / DB_NAME

# The one region this pipeline actually forecasts carbon intensity for:
# matches mart_best_hours_today's own hardcoded home_region_id = 8 and
# carbon_intensity.py's --region default. Change all three together if
# that ever changes. Generation mix has no region breakdown at all: Elexon
# publishes FUELHH for the whole GB transmission system, not per region.
HOME_REGION_ID = 8
HOME_REGION_NAME = "West Midlands"

# Fixed per-category colours, not cycled from data: a category keeps its
# colour across every reload rather than being reassigned when a filter
# changes which categories are on screen. Slots taken from the project's
# validated categorical order (dataviz skill), picked for a plausible read:
# green for renewable, orange for fossil.
FUEL_CATEGORY_COLORS = {
    "Low carbon": "#2a78d6",
    "Fossil": "#eb6834",
    "Interconnector": "#1baf7a",
    "Storage": "#eda100",
    "Other": "#e87ba4",
    "Renewable": "#008300",
    # dim_technology's one category with no equivalent in the Elexon/CI
    # taxonomies this palette was built for (Demand, Reactive
    # Compensation, Substation: real queue entries, not a generation or
    # storage technology). Same muted grey as EVENT_COLORS' "Normal":
    # recedes on a chart the same way an unremarkable case should.
    "Non-generation": "#c3c2b7",
}

# Status colours, reserved for exactly this: flagging a period as notable,
# never reused as a generic series colour.
EVENT_COLORS = {
    "Negative price": "#0ca30c",  # good: system is paying to take power
    "Large swing": "#ec835a",  # serious: a real move worth noticing
    "Normal": "#c3c2b7",  # recedes; most periods are unremarkable
}

# A half-hour-on-half-hour move of this size or more counts as a "swing".
# A simple, readable threshold for a personal dashboard, not a calibrated
# statistical definition.
NOTABLE_SWING_GBP_PER_MWH = 50


@st.cache_data(ttl=300)
def run_query(sql: str, params: list | None = None) -> pd.DataFrame:
    """Read-only connection: the dashboard must never hold DuckDB's single
    write lock, or a dbt run on this same file would block behind it.
    Cached for 5 minutes: gold only moves on the half-hourly schedule
    anyway, so re-querying on every widget interaction buys nothing.
    Parameters are bound, not string-formatted, even though today's only
    caller passes values Streamlit widgets already validated (a date, a
    timestamp). One query-running function for the whole app is worth
    keeping safe by construction rather than trusted by inspection."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql, params or []).fetchdf()
    finally:
        con.close()


def render_regional_greenness() -> None:
    df = run_query(f"""
        select half_hour_start_utc, intensity_forecast_gco2_per_kwh, intensity_index
        from main_gold.fct_regional_intensity
        where region_id = {HOME_REGION_ID}
          and half_hour_start_utc >= date_trunc('hour', now()) - interval 1 hour
        order by half_hour_start_utc
    """)
    if df.empty:
        st.info(
            f"No carbon intensity forecast for {HOME_REGION_NAME} yet. Fetch fresh "
            "data and rebuild gold (`make build-local`) to populate it."
        )
        return

    st.write(
        f"Forecast carbon intensity for **{HOME_REGION_NAME}**, your home region, "
        "from now out to however far ahead the Carbon Intensity API's forecast "
        "currently reaches (up to 48 hours). Lower gCO2/kWh is cleaner."
    )

    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    so_far = df[df["half_hour_start_utc"] <= now]
    current = so_far.iloc[-1] if not so_far.empty else df.iloc[0]

    # index is the Carbon Intensity API's own published band, carried
    # through unchanged from bronze (see fct_regional_intensity's model
    # comment), not a threshold guessed at in this app. delta_color
    # leans on st.metric's built-in green/red rather than hand-rolled
    # HTML: "normal" for the two clean bands, "inverse" for the two dirty
    # ones, "off" (grey, no arrow) for moderate.
    delta_color = {"very low": "normal", "low": "normal", "moderate": "off"}.get(
        current["intensity_index"], "inverse"
    )

    cols = st.columns(2)
    cols[0].metric(
        "Right now",
        f"{current['intensity_forecast_gco2_per_kwh']:.0f} gCO2/kWh",
        current["intensity_index"].title(),
        delta_color=delta_color,
    )
    cols[1].metric(
        "Forecast reaches",
        df["half_hour_start_utc"].max().strftime("%a %H:%M"),
        f"{(df['half_hour_start_utc'].max() - now).total_seconds() / 3600:.0f}h ahead",
        delta_color="off",
    )
    st.caption(f"As of {current['half_hour_start_utc'].strftime('%H:%M')} UTC.")

    fig = px.line(
        df,
        x="half_hour_start_utc",
        y="intensity_forecast_gco2_per_kwh",
        hover_data={"intensity_index": True},
        labels={"half_hour_start_utc": "", "intensity_forecast_gco2_per_kwh": "gCO2/kWh", "intensity_index": "Band"},
    )
    fig.update_traces(line_color=FUEL_CATEGORY_COLORS["Renewable"])
    fig.add_vline(x=now, line_dash="dash", line_color="#898781")
    st.plotly_chart(fig, width="stretch")

    mix = run_query(f"""
        with latest as (
            select max(half_hour_start_utc) as ts
            from main_gold.fct_regional_generation_mix
            where region_id = {HOME_REGION_ID} and half_hour_start_utc <= now()
        )
        select f.ci_fuel_name, f.fuel_category, m.mix_pct
        from main_gold.fct_regional_generation_mix m
        join main_gold.dim_ci_fuel_type f using (ci_fuel_type_key)
        join latest on m.half_hour_start_utc = latest.ts
        where m.region_id = {HOME_REGION_ID}
        order by m.mix_pct desc
    """)
    if not mix.empty:
        st.write(f"What that intensity is made of, right now, in {HOME_REGION_NAME}:")
        fig = px.bar(
            mix,
            x="mix_pct",
            y="ci_fuel_name",
            orientation="h",
            color="fuel_category",
            color_discrete_map=FUEL_CATEGORY_COLORS,
            labels={"mix_pct": "Share of local mix (%)", "ci_fuel_name": "", "fuel_category": "Category"},
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, width="stretch")


def render_generation_mix() -> None:
    bounds = run_query(
        "select min(settlement_date) as earliest, max(settlement_date) as latest from main_gold.fct_generation"
    )
    if bounds.empty or pd.isna(bounds["latest"].iloc[0]):
        st.info("No generation data yet. Run the pipeline to land some.")
        return
    earliest_date = bounds["earliest"].iloc[0].date()
    latest_date = bounds["latest"].iloc[0].date()

    st.write(
        "What's actually powering Great Britain's grid, across the whole "
        "transmission system. Elexon doesn't publish this broken down by "
        f"region, unlike the carbon forecast, which is specific to {HOME_REGION_NAME} "
        "(see the first tab)."
    )

    picked_date = st.date_input("Date", value=latest_date, min_value=earliest_date, max_value=latest_date)
    periods = run_query(
        "select distinct settlement_period_start_utc from main_gold.fct_generation "
        "where settlement_date = ? order by settlement_period_start_utc",
        params=[picked_date],
    )
    if periods.empty:
        st.info(f"No generation data for {picked_date}.")
        return

    times = periods["settlement_period_start_utc"]
    picked_time = st.select_slider(
        "Settlement period", options=list(times), value=times.iloc[-1], format_func=lambda t: t.strftime("%H:%M")
    )

    df = run_query(
        """
        select f.fuel_type_name, f.fuel_category, g.generation_mw, g.share_of_mix_pct
        from main_gold.fct_generation g
        join main_gold.dim_fuel_type f using (fuel_type_key)
        where g.settlement_period_start_utc = ?
        """,
        params=[picked_time],
    )
    st.caption(f"{picked_time} UTC")

    # Net-mix shares (interconnectors included in the denominator, see
    # fct_generation's own comment) can run negative or over 100%, shown
    # as a running total below rather than clamped away, so a category
    # this dashboard doesn't call out by name is never silently missing.
    by_category = (
        df.groupby("fuel_category", as_index=False)[["generation_mw", "share_of_mix_pct"]]
        .sum()
        .sort_values("share_of_mix_pct", ascending=False)
    )

    metric_cols = st.columns(len(by_category))
    for col, (_, row) in zip(metric_cols, by_category.iterrows(), strict=False):
        col.metric(row["fuel_category"], f"{row['share_of_mix_pct']:.0f}%")
    st.caption(f"All {len(by_category)} categories shown above, totalling {by_category['share_of_mix_pct'].sum():.0f}%.")

    by_fuel_type = st.toggle("Break down by individual fuel type", value=False)
    if by_fuel_type:
        chart_df = df.sort_values("share_of_mix_pct", ascending=False)
        y_col = "fuel_type_name"
        show_legend = True
    else:
        chart_df = by_category
        y_col = "fuel_category"
        show_legend = False

    fig = px.bar(
        chart_df,
        x="share_of_mix_pct",
        y=y_col,
        orientation="h",
        color="fuel_category",
        color_discrete_map=FUEL_CATEGORY_COLORS,
        hover_data={"generation_mw": ":,.0f", "fuel_category": show_legend},
        labels={
            "share_of_mix_pct": "Share of mix (%)",
            "generation_mw": "MW",
            "fuel_category": "Category",
            y_col: "",
        },
    )
    fig.update_layout(showlegend=show_legend, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, width="stretch")


def render_best_hours() -> None:
    df = run_query("select * from main_gold.mart_best_hours_today order by half_hour_start_utc")
    if df.empty:
        st.info(
            "No forecast for the next 24 hours yet. mart_best_hours_today is built "
            "from carbon_intensity's forecast, which only reaches 48 hours out. "
            "Fetch fresh data and rebuild gold (`make build-local`) to populate it."
        )
        return

    region = df["region_short_name"].iloc[0]
    st.write(
        f"The next 24 hours in **{region}**, your home region, ranked by forecast "
        "carbon intensity: lower gCO2/kWh means a cleaner half hour to use "
        "electricity. Price only appears once a half hour has actually settled, "
        "a few hours behind real time, so most of the next 24 hours have no price "
        "yet. That's expected, not missing data."
    )
    st.caption(f"Generated {df['generated_at'].iloc[0]}")

    max_hours = len(df) // 2
    if max_hours > 4:
        # range() stops at max_hours itself only when it happens to be even
        # (50 rows -> 25 hours, an odd number a step-2 range never lands
        # on). Append it explicitly rather than silently rounding the
        # default down to the nearest even option.
        hour_options = list(range(2, max_hours, 2)) + [max_hours]
        hours_ahead = st.select_slider("Look ahead", options=hour_options, value=max_hours)
        df = df.head(hours_ahead * 2)

    # Ranked directly off the (possibly windowed) intensity/price columns
    # rather than the mart's own greenness_rank/cheapness_rank, which are
    # computed over the full 24 hours and would point outside a shorter
    # window once the slider narrows it.
    greenest = df.loc[df["intensity_forecast_gco2_per_kwh"].idxmin()]
    cols = st.columns(2)
    cols[0].metric(
        "Greenest hour",
        greenest["half_hour_start_utc"].strftime("%H:%M"),
        f"{greenest['intensity_forecast_gco2_per_kwh']:.0f} gCO2/kWh",
    )

    priced = df.dropna(subset=["system_sell_price_gbp_per_mwh"])
    if priced.empty:
        cols[1].metric("Cheapest settled hour", "—", "no settled price yet")
    else:
        cheapest = priced.loc[priced["system_sell_price_gbp_per_mwh"].idxmin()]
        cols[1].metric(
            "Cheapest settled hour",
            cheapest["half_hour_start_utc"].strftime("%H:%M"),
            f"£{cheapest['system_sell_price_gbp_per_mwh']:.0f}/MWh",
        )

    fig = px.bar(
        df,
        x="half_hour_start_utc",
        y="intensity_forecast_gco2_per_kwh",
        color="intensity_forecast_gco2_per_kwh",
        color_continuous_scale="Blues",
        hover_data={"system_sell_price_gbp_per_mwh": ":.0f", "greenness_rank": True},
        labels={
            "half_hour_start_utc": "",
            "intensity_forecast_gco2_per_kwh": "gCO2/kWh",
            "system_sell_price_gbp_per_mwh": "£/MWh",
            "greenness_rank": "Rank (of 24h)",
        },
    )
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch")

    st.dataframe(
        df.nsmallest(5, "intensity_forecast_gco2_per_kwh")[
            ["half_hour_start_utc", "intensity_forecast_gco2_per_kwh", "system_sell_price_gbp_per_mwh"]
        ],
        hide_index=True,
    )


def _label_event(row: pd.Series) -> str:
    if row["system_sell_price_gbp_per_mwh"] < 0:
        return "Negative price"
    change = row["price_change_gbp_per_mwh"]
    if pd.notna(change) and abs(change) >= NOTABLE_SWING_GBP_PER_MWH:
        return "Large swing"
    return "Normal"


def render_price_events() -> None:
    # Fetches a generous 120-period window so lag() has a real previous
    # value for every one of the (at most 100) periods a viewer can select
    # below. lag() runs over the window's own ascending order regardless
    # of how the outer query is sorted, so this reads fine sorted newest
    # first for display.
    df = run_query("""
        with recent as (
            select settlement_period_start_utc, system_sell_price_gbp_per_mwh
            from main_gold.fct_settlement_period
            where system_sell_price_gbp_per_mwh is not null
            order by settlement_period_start_utc desc
            limit 120
        )
        select *,
            system_sell_price_gbp_per_mwh
                - lag(system_sell_price_gbp_per_mwh) over (order by settlement_period_start_utc)
                as price_change_gbp_per_mwh
        from recent
        order by settlement_period_start_utc desc
        limit 100
    """)
    if df.empty:
        st.info("No settled prices yet. Run the pipeline to land some.")
        return

    st.write(
        "GB's system sell price, set nationally for the whole grid rather than "
        "per region, for each half-hour settlement period as it's settled, a "
        "few hours behind real time. Flagged below: half hours the system "
        "actually paid to take power (a negative price), and swings of "
        f"£{NOTABLE_SWING_GBP_PER_MWH}/MWh or more from the half hour before."
    )

    periods_shown = st.select_slider("Show last", options=[10, 20, 50, 100], value=20)
    df = df.head(periods_shown)
    df["event"] = df.apply(_label_event, axis=1)

    event_types = st.multiselect(
        "Event types", options=["Negative price", "Large swing", "Normal"],
        default=["Negative price", "Large swing", "Normal"],
    )
    df = df[df["event"].isin(event_types)]
    if df.empty:
        st.caption("Nothing matches that filter in the selected periods.")
        return

    fig = px.bar(
        df.sort_values("settlement_period_start_utc"),
        x="settlement_period_start_utc",
        y="system_sell_price_gbp_per_mwh",
        color="event",
        color_discrete_map=EVENT_COLORS,
        hover_data={"price_change_gbp_per_mwh": ":+.1f"},
        labels={
            "settlement_period_start_utc": "",
            "system_sell_price_gbp_per_mwh": "£/MWh",
            "price_change_gbp_per_mwh": "Change from previous",
            "event": "",
        },
    )
    st.plotly_chart(fig, width="stretch")

    events = df[df["event"] != "Normal"]
    if events.empty:
        st.caption(f"No notable price events in the last {periods_shown} settled periods, the healthy state.")
    else:
        st.dataframe(
            events[
                ["settlement_period_start_utc", "system_sell_price_gbp_per_mwh", "price_change_gbp_per_mwh", "event"]
            ],
            hide_index=True,
        )


def render_connections_queue() -> None:
    evolution = run_query("select * from main_gold.mart_queue_evolution order by as_of_month")
    if evolution.empty:
        st.info(
            "No connections queue data yet. Run `uv run python -m lakehouse.extractors.neso_connections` "
            "and rebuild gold (`make build-local`) to populate it."
        )
        return

    latest = evolution.iloc[-1]
    st.write(
        "NESO's Transmission Entry Capacity register: every project with a contract to connect "
        "to the GB transmission system, and how the queue itself is moving. NESO only publishes "
        "the current snapshot, twice a week; this dashboard is what turns that into history."
    )
    st.caption(f"As of {latest['as_of_month'].strftime('%B %Y')}, {len(evolution)} monthly snapshot(s) of history so far.")

    cols = st.columns(3)
    cols[0].metric("Projects in queue", f"{latest['projects_in_queue']:,.0f}")
    cols[1].metric("Total contracted capacity", f"{latest['total_capacity_mw'] / 1000:.1f} GW")
    if pd.notna(latest["renewable_share_of_generation_pct"]):
        cols[2].metric("Renewable share of generation", f"{latest['renewable_share_of_generation_pct']:.0f}%")
    else:
        cols[2].metric("Renewable share of generation", "—")

    if len(evolution) > 1:
        fig = px.area(
            evolution,
            x="as_of_month",
            y="total_capacity_mw",
            labels={"as_of_month": "", "total_capacity_mw": "MW"},
        )
        fig.update_traces(line_color=FUEL_CATEGORY_COLORS["Renewable"])
        st.plotly_chart(fig, width="stretch")
    else:
        st.caption(
            "Queue size over time needs at least two monthly snapshots to plot; there is "
            "only one so far. NESO republishes twice a week, so this fills in from here."
        )

    st.write("What's actually in the queue right now, by technology:")
    mix = run_query("""
        select t.technology_category, sum(f.cumulative_capacity_mw) as capacity_mw
        from main_gold.fct_connection_queue f
        join main_gold.dim_technology t using (technology_key)
        where f.as_of_month = (select max(as_of_month) from main_gold.fct_connection_queue)
        group by 1
        order by 2 desc
    """)
    fig = px.bar(
        mix,
        x="capacity_mw",
        y="technology_category",
        orientation="h",
        color="technology_category",
        color_discrete_map=FUEL_CATEGORY_COLORS,
        labels={"capacity_mw": "Contracted capacity (MW)", "technology_category": ""},
    )
    fig.update_layout(showlegend=False, yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, width="stretch")

    avg_year = run_query("""
        select avg(year(mw_effective_from)) as avg_year
        from main_gold.fct_connection_queue
        where as_of_month = (select max(as_of_month) from main_gold.fct_connection_queue)
          and mw_effective_from is not null
    """)["avg_year"].iloc[0]
    if pd.notna(avg_year):
        st.caption(
            f"Average published connection year across the current queue: **{avg_year:.0f}**. "
            "NULL for any project with no published effective date yet, excluded here rather than "
            "treated as zero."
        )

    st.write("Movement since the previous monthly snapshot:")
    if latest["projects_with_known_slippage"] == 0:
        st.caption(
            "No month-on-month comparison possible yet: connection_date_slippage_days only "
            "exists for a project seen in two consecutive monthly snapshots, and there is only "
            "one so far. Check back after the next snapshot."
        )
    else:
        move_cols = st.columns(3)
        move_cols[0].metric("Projects with a known move", f"{latest['projects_with_known_slippage']:,.0f}")
        move_cols[1].metric("Delayed", f"{latest['projects_delayed']:,.0f}")
        move_cols[2].metric("Brought forward", f"{latest['projects_accelerated']:,.0f}")
        if pd.notna(latest["avg_connection_date_slippage_days"]):
            direction = "later" if latest["avg_connection_date_slippage_days"] > 0 else "earlier"
            st.caption(
                f"Average move: {abs(latest['avg_connection_date_slippage_days']):.0f} days {direction} "
                "than the date published the previous month."
            )


def render_tariff_comparison() -> None:
    df = run_query("select * from main_gold.mart_tariff_comparison order by period_start")
    if df.empty:
        st.info(
            "No usage logged yet. Add a row to data/manual/electricity_usage.csv from your "
            "supplier's own app (date range, day kWh, night kWh, estimated cost), then run "
            "manual_usage_job from the Dagster UI. See lakehouse.extractors.manual_usage's "
            "module docstring for the exact format."
        )
        return

    st.write(
        "Would Octopus Agile have cost less than what you actually paid, for the electricity "
        "you actually used? Compared period by period, using whatever day/night totals you've "
        "logged from your supplier's app, against Agile's real published half-hourly rates for "
        "the same dates."
    )
    st.caption(
        "A real approximation, not hidden: without half-hourly consumption, this assumes your "
        "day-band usage was spread evenly across the period's day half hours (and the same for "
        "night), the best a day/night split can support. It is not the same claim as \"every "
        "half hour's actual usage times its actual price.\""
    )

    total_actual = df["actual_estimated_cost_gbp"].sum()
    total_agile = df["agile_equivalent_cost_gbp"].sum(skipna=True)
    total_savings = total_actual - total_agile if df["agile_equivalent_cost_gbp"].notna().all() else None

    cols = st.columns(3)
    cols[0].metric("Actual cost logged", f"£{total_actual:.2f}")
    if pd.notna(total_agile) and df["agile_equivalent_cost_gbp"].notna().any():
        cols[1].metric("Agile-equivalent cost", f"£{total_agile:.2f}")
    else:
        cols[1].metric("Agile-equivalent cost", "—")
    if total_savings is not None:
        label = "Agile would have saved" if total_savings > 0 else "Agile would have cost more"
        cols[2].metric(label, f"£{abs(total_savings):.2f}", delta_color="off")
    else:
        cols[2].metric("Agile vs actual", "incomplete data")

    if not df["is_complete"].all():
        st.caption(
            "One or more periods above have partial Agile price history (logged further back "
            "than prices have been backfilled). Their numbers reflect only the coverage available, "
            "not the whole period."
        )

    fig = px.bar(
        df,
        x=df["period_start"].astype(str) + " to " + df["period_end"].astype(str),
        y=["actual_estimated_cost_gbp", "agile_equivalent_cost_gbp"],
        barmode="group",
        labels={"value": "£", "x": "", "variable": ""},
    )
    fig.for_each_trace(
        lambda t: t.update(
            name={"actual_estimated_cost_gbp": "Actual (your app)", "agile_equivalent_cost_gbp": "Agile-equivalent"}.get(
                t.name, t.name
            )
        )
    )
    st.plotly_chart(fig, width="stretch")

    st.dataframe(
        df[
            [
                "period_start",
                "period_end",
                "day_kwh",
                "night_kwh",
                "actual_estimated_cost_gbp",
                "agile_equivalent_cost_gbp",
                "agile_savings_gbp",
                "is_complete",
            ]
        ],
        hide_index=True,
    )

    st.info(
        "Not answered here: whether following this dashboard's own cheapest-hours advice "
        "actually saved anything this month. That needs evidence of when you actually used "
        "power, not just a daily or weekly total — this project has no half-hourly smart-meter "
        "export to check that against (see README's \"The money story\"), so this stays an open "
        "question rather than a number invented to fill the gap."
    )


def main() -> None:
    st.set_page_config(page_title="Energy Watch", page_icon="⚡", layout="wide")
    st.title("Energy Watch")
    st.caption(
        f"What's on the GB grid right now, and when it's cheapest and cleanest "
        f"to use it. Home region for the forecast: {HOME_REGION_NAME}."
    )

    tabs = st.tabs(
        [
            "How green is it?",
            f"Best hours today · {HOME_REGION_NAME}",
            "GB generation mix",
            "Recent price events",
            "Connections queue",
            "The money story",
        ]
    )
    with tabs[0]:
        render_regional_greenness()
    with tabs[1]:
        render_best_hours()
    with tabs[2]:
        render_generation_mix()
    with tabs[3]:
        render_price_events()
    with tabs[4]:
        render_connections_queue()
    with tabs[5]:
        render_tariff_comparison()


if __name__ == "__main__":
    main()

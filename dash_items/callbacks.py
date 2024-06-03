from datetime import datetime
from dash import callback, Output, Input
import plotly.express as px
from pyspark.sql import SparkSession
from os import path
from .component_ids import (
    MFR_MAPPER_ID,
    DATE_PICKER_ID,
    FARM_PICKER_ID,
    MFR_KPI_PICKER_ID,
    MFR_CONTAINER_ID,
    FEED_CONTAINER_ID,
    FEED_KPI_PICKER_ID,
    FEED_MAPPER_ID,
    RATION_CONTAINER_ID,
    RATION_MAPPER_ID,
    RATION_KPI_PICKER_ID,
)

DATE_FORMAT = "%Y-%m-%d"


@callback(
    [
        Output(component_id=MFR_CONTAINER_ID, component_property="children"),
        Output(component_id=MFR_MAPPER_ID, component_property="figure"),
    ],
    [
        Input(component_id=DATE_PICKER_ID, component_property="start_date"),
        Input(component_id=DATE_PICKER_ID, component_property="end_date"),
        Input(component_id=FARM_PICKER_ID, component_property="value"),
        Input(component_id=MFR_KPI_PICKER_ID, component_property="value"),
    ],
)
def update_mfr_line_chart(
    selected_start_date,
    selected_end_date,
    selected_farm,
    selected_kpi,
):
    spark = SparkSession.builder.getOrCreate()
    mfr_daily_fact_table_path = path.abspath("spark-warehouse/gold/mfr_daily_fact")
    container = ""

    dff = spark.read.load(mfr_daily_fact_table_path).toPandas()

    # in case only one farm is selected
    if selected_farm is not None:
        if isinstance(selected_farm, str):
            selected_farm = [selected_farm]
        dff = dff[dff["farm_license"].isin(selected_farm)]
    if selected_start_date is not None:
        start_date = datetime.strptime(selected_start_date, DATE_FORMAT).date()
        dff = dff[dff["date"] >= start_date]
    if selected_end_date is not None:
        end_date = datetime.strptime(selected_end_date, DATE_FORMAT).date()
        dff = dff[dff["date"] <= end_date]
    if selected_kpi is not None:
        dff = (
            dff.groupby(["farm_license", "system_number", "date"])
            # the table is already aggregated by the columns above,
            # so it doesn't matter which agg function we use
            [[selected_kpi]].sum()
        )
        dff.reset_index(inplace=True)

    # Plotly Express
    try:
        fig = px.line(
            data_frame=dff,
            x="date",
            y=selected_kpi,
            color="farm_license",
            markers=True,
            title=selected_kpi,
        )
    except Exception:
        fig = px.line(
            data_frame=dff,
            x="date",
            y=selected_kpi,
            color="farm_license",
            markers=True,
            title=selected_kpi,
        )
    fig.update_layout(yaxis_title_text=selected_kpi)

    return container, fig


@callback(
    [
        Output(component_id=FEED_CONTAINER_ID, component_property="children"),
        Output(component_id=FEED_MAPPER_ID, component_property="figure"),
    ],
    [
        Input(component_id=DATE_PICKER_ID, component_property="start_date"),
        Input(component_id=DATE_PICKER_ID, component_property="end_date"),
        Input(component_id=FARM_PICKER_ID, component_property="value"),
        Input(component_id=FEED_KPI_PICKER_ID, component_property="value"),
    ],
)
def update_feed_line_chart(
    selected_start_date,
    selected_end_date,
    selected_farm,
    selected_kpi,
):
    spark = SparkSession.builder.getOrCreate()
    feed_daily_fact_table_path = path.abspath("spark-warehouse/gold/feed_daily_fact")
    container = ""

    dff = spark.read.load(feed_daily_fact_table_path).toPandas()

    # in case only one farm is selected
    if selected_farm is not None:
        if isinstance(selected_farm, str):
            selected_farm = [selected_farm]
        dff = dff[dff["farm_license"].isin(selected_farm)]
    if selected_start_date is not None:
        start_date = datetime.strptime(selected_start_date, DATE_FORMAT).date()
        dff = dff[dff["date"] >= start_date]
    if selected_end_date is not None:
        end_date = datetime.strptime(selected_end_date, DATE_FORMAT).date()
        dff = dff[dff["date"] <= end_date]
    if selected_kpi is not None:
        dff = (
            dff.groupby(["farm_license", "system_number", "feedName", "date"])
            # the table is already aggregated by the columns above,
            # so it doesn't matter which agg function we use
            [[selected_kpi]].sum()
        )
        dff.reset_index(inplace=True)

    # Plotly Express
    try:
        fig = px.line(
            data_frame=dff,
            facet_col="farm_license",
            x="date",
            y=selected_kpi,
            color="feedName",
            markers=True,
            title=selected_kpi,
        )
    except Exception:
        fig = px.line(
            data_frame=dff,
            facet_col="farm_license",
            x="date",
            y=selected_kpi,
            color="feedName",
            markers=True,
            title=selected_kpi,
        )
    fig.update_layout(yaxis_title_text=selected_kpi)

    return container, fig


@callback(
    [
        Output(component_id=RATION_CONTAINER_ID, component_property="children"),
        Output(component_id=RATION_MAPPER_ID, component_property="figure"),
    ],
    [
        Input(component_id=DATE_PICKER_ID, component_property="start_date"),
        Input(component_id=DATE_PICKER_ID, component_property="end_date"),
        Input(component_id=FARM_PICKER_ID, component_property="value"),
        Input(component_id=RATION_KPI_PICKER_ID, component_property="value"),
    ],
)
def update_ration_line_chart(
    selected_start_date,
    selected_end_date,
    selected_farm,
    selected_kpi,
):
    spark = SparkSession.builder.getOrCreate()
    ration_daily_fact_table_path = path.abspath(
        "spark-warehouse/gold/ration_daily_fact"
    )
    container = ""

    dff = spark.read.load(ration_daily_fact_table_path).toPandas()

    # in case only one farm is selected
    if selected_farm is not None:
        if isinstance(selected_farm, str):
            selected_farm = [selected_farm]
        dff = dff[dff["farm_license"].isin(selected_farm)]
    if selected_start_date is not None:
        start_date = datetime.strptime(selected_start_date, DATE_FORMAT).date()
        dff = dff[dff["date"] >= start_date]
    if selected_end_date is not None:
        end_date = datetime.strptime(selected_end_date, DATE_FORMAT).date()
        dff = dff[dff["date"] <= end_date]
    if selected_kpi is not None:
        dff = (
            dff.groupby(["farm_license", "system_number", "rationName", "date"])
            # the table is already aggregated by the columns above,
            # so it doesn't matter which agg function we use
            [[selected_kpi]].sum()
        )
        dff.reset_index(inplace=True)

    # Plotly Express
    try:
        fig = px.line(
            data_frame=dff,
            facet_col="farm_license",
            x="date",
            y=selected_kpi,
            color="rationName",
            markers=True,
            title=selected_kpi,
        )
    except Exception:
        fig = px.line(
            data_frame=dff,
            facet_col="farm_license",
            x="date",
            y=selected_kpi,
            color="rationName",
            markers=True,
            title=selected_kpi,
        )
    fig.update_layout(yaxis_title_text=selected_kpi)

    return container, fig

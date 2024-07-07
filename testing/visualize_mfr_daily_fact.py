from jobs.job import Job
from dash import Dash, html
from dash_items.component_generators import (
    generate_date_picker_range_component,
    generate_dropdown_component_with_farms,
    generate_radio_button_component_with_kpis,
    generate_graph,
)
from dash_items.component_ids import (
    DATE_PICKER_ID,
    ROBOT_KPI_PICKER_ID,
    FARM_PICKER_ID,
    ROBOT_MAPPER_ID,
    ROBOT_CONTAINER_ID,
    ROBOT_KPIS,
)

# from dash_items.callbacks import update_robot_line_chart  # noqa: F401
from operations.helper_functions import extract_all_farm_licenses

import plotly.express as px
from dash import callback, Output, Input
from pyspark.sql import SparkSession, functions as F
from datetime import datetime

DATE_FORMAT = "%Y-%m-%d"


@callback(
    [
        Output(component_id=ROBOT_CONTAINER_ID, component_property="children"),
        Output(component_id=ROBOT_MAPPER_ID, component_property="figure"),
    ],
    [
        Input(component_id=DATE_PICKER_ID, component_property="start_date"),
        Input(component_id=DATE_PICKER_ID, component_property="end_date"),
        Input(component_id=FARM_PICKER_ID, component_property="value"),
        Input(component_id=ROBOT_KPI_PICKER_ID, component_property="value"),
    ],
)
def update_robot_line_chart(
    selected_start_date,
    selected_end_date,
    selected_farm,
    selected_kpi,
):
    spark = SparkSession.builder.getOrCreate()
    farm_daily_fact_table_path = "../spark-warehouse/gold/farm_daily_fact"
    container = ""

    dff = spark.read.load(farm_daily_fact_table_path).dropDuplicates().toPandas()

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
            dff.groupby(["farm_license", "date"])
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


class VisualizeRobot(Job):
    def __init__(
        self,
        config_file_path: str,
    ):
        Job.__init__(self, config_file_path)
        self.config = self.common_initialization()
        self.farm_list = extract_all_farm_licenses(
            "../spark-warehouse/bronze/farm_info"
        )

    def launch(self):
        self.logger.info("Starting VisualizeRobot Job")

        app = Dash(__name__, suppress_callback_exceptions=True)
        app.layout = html.Div(
            [
                html.H1(
                    "Web Application Dashboards with Dash",
                    style={"text-align": "center"},
                ),
                generate_date_picker_range_component(DATE_PICKER_ID),
                generate_dropdown_component_with_farms(FARM_PICKER_ID, self.farm_list),
                generate_radio_button_component_with_kpis(
                    ROBOT_KPI_PICKER_ID, ROBOT_KPIS
                ),
                *generate_graph(ROBOT_CONTAINER_ID, ROBOT_MAPPER_ID),
            ]
        )

        dff = (
            self.spark.read.load("../spark-warehouse/gold/farm_daily_fact")
            .dropDuplicates()
            .where(F.col("farm_license") == "FXD3T-ZTKZ6-RGDO1")
            .where(F.col("date") >= "2023-01-01")
            .where(F.col("date") <= "2023-01-08")
        )
        dff.sort("farm_license", "date").show()

        app.run_server(debug=True)


if __name__ == "__main__":
    config_file_path = r"../conf/load_data_from_warehouse.json"

    job = VisualizeRobot(config_file_path=config_file_path)
    job.launch()

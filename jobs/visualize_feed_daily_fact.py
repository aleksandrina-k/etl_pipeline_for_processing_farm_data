from datetime import date, datetime
from jobs.job import Job
import plotly.express as px
from dash import Dash, dcc, html, Input, Output

from operations.helper_functions import extract_all_farm_licenses

DEFAULT_START_DATE = date(2021, 1, 1)
DEFAULT_END_DATE = date(2023, 12, 31)
KPIS = [
    "loadingAccuracyPerc",
    "totalRequestedWeightKg",
    "totalLoadedWeightKg",
    "avgRequestedWeightKg",
    "avgLoadedWeightKg",
]


class VisualizeFeed(Job):
    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Visualization With Job Job")

        # -- Import and clean data
        # get all farms
        farms = extract_all_farm_licenses("../spark-warehouse/bronze/bronze_table")
        df = self.spark.read.load(
            "../spark-warehouse/gold/feed_loading_daily_fact"
        ).toPandas()

        app = Dash(__name__)

        # ------------------------------------------------------------------------------
        # App layout
        app.layout = html.Div(
            [
                html.H1(
                    "Web Application Dashboards with Dash",
                    style={"text-align": "center"},
                ),
                dcc.DatePickerSingle(
                    id="date_picker",
                    min_date_allowed=DEFAULT_START_DATE,
                    max_date_allowed=DEFAULT_END_DATE,
                    initial_visible_month=date(2023, 1, 1),
                    display_format="YYYY-MM-DD",
                    stay_open_on_select=True,
                ),
                dcc.Dropdown(
                    id="farm_picker",
                    options=[{"label": x, "value": x} for x in farms],
                    multi=False,
                    value="",
                    style={"width": "40%"},
                ),
                # TODO: Maybe add Dropdown menu for feeds
                dcc.RadioItems(
                    id="kpi_picker",
                    options=KPIS,
                    value=KPIS[0],
                ),
                # dash_table.DataTable(data=df.to_dict("records"), page_size=15),
                html.Div(id="output_container", children=[]),
                html.Br(),
                dcc.Graph(id="feed_loading_daily_fact_map", figure={}),
            ]
        )

        # ------------------------------------------------------------------------------
        # Connect the Plotly graphs with Dash Components
        @app.callback(
            [
                Output(component_id="output_container", component_property="children"),
                Output(
                    component_id="feed_loading_daily_fact_map",
                    component_property="figure",
                ),
            ],
            [
                Input(component_id="date_picker", component_property="date"),
                Input(component_id="farm_picker", component_property="value"),
                Input(component_id="kpi_picker", component_property="value"),
            ],
        )
        def update_graph(
            selected_date_option,
            selected_farm_option,
            selected_kpi_option,
        ):

            # container = f"Display {selected_kpi_option} KPI for {selected_farm_option} for time period " \
            #             f"[{option_slctd_start_date} - {option_slctd_end_date}]"
            container = ""
            dff = df.copy()

            # in case only one farm is selected
            if selected_farm_option is not None:
                if isinstance(selected_farm_option, str):
                    selected_farm_option = [selected_farm_option]
                dff = dff[dff["farm_license"].isin(selected_farm_option)]
            if selected_date_option is not None:
                date = datetime.strptime(selected_date_option, "%Y-%m-%d").date()
                dff = dff[dff["date"] == date]
            if selected_kpi_option is not None:
                dff = (
                    dff.groupby(["farm_license", "system_number", "date", "feedName"])
                    # the table is already aggregated by the columns above,
                    # so it doesn't matter which agg function we use
                    [[selected_kpi_option]].sum()
                )
                dff.reset_index(inplace=True)

            # Plotly Express
            fig = px.histogram(
                data_frame=dff,
                x="feedName",
                y=selected_kpi_option,
                title=selected_kpi_option,
            )

            return container, fig

        app.run_server(debug=True)


if __name__ == "__main__":
    config_file_path = r"../conf/load_data_from_warehouse.json"

    job = VisualizeFeed(config_file_path)
    job.launch()

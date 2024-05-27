from datetime import date, datetime
from jobs.job import Job
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
from operations.helper_functions import extract_all_farm_licenses

DEFAULT_START_DATE = date(2021, 1, 1)
DEFAULT_END_DATE = date(2023, 12, 31)
KPIS = [
    "totalLoadingSpeedKgPerH",
    "totalRequestedWeightKg",
    "totalLoadedWeightKg",
    "avgNrOfFeedInRation",
    "totalNrOfFeedPerLoad",
    "totalNrBinsLoaded",
    "loadingAccuracyPerc",
]


class VisualizeMfr(Job):
    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Visualization With Job Job")

        # Import the data
        farms = extract_all_farm_licenses("../spark-warehouse/bronze/bronze_table")
        df = self.spark.read.load(
            "../spark-warehouse/gold/ration_daily_fact"
        ).toPandas()

        app = Dash(__name__)
        app.layout = html.Div(
            [
                html.H1(
                    "Web Application Dashboards with Dash",
                    style={"text-align": "center"},
                ),
                dcc.DatePickerRange(
                    id="date_picker",
                    min_date_allowed=DEFAULT_START_DATE,
                    max_date_allowed=DEFAULT_END_DATE,
                    start_date=DEFAULT_START_DATE,
                    end_date=DEFAULT_START_DATE,
                    initial_visible_month=date(2023, 1, 1),
                    display_format="YYYY-MM-DD",
                    end_date_placeholder_text="Select a date!",
                ),
                dcc.Dropdown(
                    id="farm_picker",
                    options=[{"label": x, "value": x} for x in farms],
                    multi=True,
                    value="",
                    style={"width": "40%"},
                ),
                dcc.RadioItems(id="kpi_picker", options=KPIS, value=KPIS[0]),
                html.Div(id="output_container", children=[]),
                html.Br(),
                dcc.Graph(id="ration_daily_fact_map", figure={}),
            ]
        )

        # ------------------------------------------------------------------------------
        # Connect the Plotly graphs with Dash Components
        @app.callback(
            [
                Output(component_id="output_container", component_property="children"),
                Output(
                    component_id="ration_daily_fact_map", component_property="figure"
                ),
            ],
            [
                Input(component_id="date_picker", component_property="start_date"),
                Input(component_id="date_picker", component_property="end_date"),
                Input(component_id="farm_picker", component_property="value"),
                Input(component_id="kpi_picker", component_property="value"),
            ],
        )
        def update_graph(
            selected_start_date,
            selected_end_date,
            selected_farm,
            selected_kpi,
        ):

            # container = f"Display {selected_kpi} KPI for {selected_farm} for time period " \
            #             f"[{selected_start_date} - {selected_end_date}]"
            container = ""
            dff = df.copy()

            # in case only one farm is selected
            if selected_farm is not None:
                if isinstance(selected_farm, str):
                    selected_farm = [selected_farm]
                dff = dff[dff["farm_license"].isin(selected_farm)]
            if selected_start_date is not None:
                start_date = datetime.strptime(selected_start_date, "%Y-%m-%d").date()
                dff = dff[dff["date"] >= start_date]
            if selected_end_date is not None:
                end_date = datetime.strptime(selected_end_date, "%Y-%m-%d").date()
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

        app.run_server(debug=True)


if __name__ == "__main__":
    config_file_path = r"../conf/load_data_from_warehouse.json"

    job = VisualizeMfr(config_file_path)
    job.launch()

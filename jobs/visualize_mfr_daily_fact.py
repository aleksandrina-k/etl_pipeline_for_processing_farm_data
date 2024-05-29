from datetime import date, datetime
from jobs.job import Job
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
from operations.helper_functions import extract_all_farm_licenses

DEFAULT_START_DATE = date(2021, 1, 1)
DEFAULT_END_DATE = date(2023, 12, 31)
MFR_KPIS = [
    "loadingAccuracyPerc",
    "totalRequestedWeightKg",
    "totalLoadedWeightKg",
    "nrSchneiderFreqControl",
    "nrCommskFreqControl",
]


class VisualizeMfr(Job):
    def __init__(
        self,
        source_table_location: str,
        farm_table_location: str,
        output_container_id: str,
        mapped_id: str,
        date_picker_id: str,
        farm_picker_id: str,
        kpi_picker_id: str,
        config_file_path: str,
        kpi_list: list[str],
        spark=None,
    ):
        Job.__init__(self, config_file_path, spark)
        self.source_table_location = source_table_location
        self.farm_table_location = farm_table_location
        self.output_container_id = output_container_id
        self.mapper_id = mapped_id
        self.date_picker_id = date_picker_id
        self.farm_picker_id = farm_picker_id
        self.kpi_picker_id = kpi_picker_id
        self.kpi_list = kpi_list

    def launch(self):
        self.logger.info("Starting VisualizeMfr Job")

        # Import the data
        farms = extract_all_farm_licenses(self.farm_table_location)
        df = self.spark.read.load(self.source_table_location).toPandas()

        app = Dash(__name__)
        app.layout = html.Div(
            [
                html.H1(
                    "Web Application Dashboards with Dash",
                    style={"text-align": "center"},
                ),
                dcc.DatePickerRange(
                    id=self.date_picker_id,
                    min_date_allowed=DEFAULT_START_DATE,
                    max_date_allowed=DEFAULT_END_DATE,
                    start_date=DEFAULT_START_DATE,
                    end_date=DEFAULT_START_DATE,
                    initial_visible_month=date(2023, 1, 1),
                    display_format="YYYY-MM-DD",
                    end_date_placeholder_text="Select a date!",
                ),
                dcc.Dropdown(
                    id=self.farm_picker_id,
                    options=[{"label": x, "value": x} for x in farms],
                    multi=True,
                    value="",
                    style={"width": "40%"},
                ),
                dcc.RadioItems(
                    id=self.kpi_picker_id, options=self.kpi_list, value=self.kpi_list[0]
                ),
                html.Div(id=self.output_container_id, children=[]),
                html.Br(),
                dcc.Graph(id=self.mapper_id, figure={}),
            ]
        )

        # ------------------------------------------------------------------------------
        # Connect the Plotly graphs with Dash Components
        @app.callback(
            [
                Output(
                    component_id=self.output_container_id, component_property="children"
                ),
                Output(component_id=self.mapper_id, component_property="figure"),
            ],
            [
                Input(
                    component_id=self.date_picker_id, component_property="start_date"
                ),
                Input(component_id=self.date_picker_id, component_property="end_date"),
                Input(component_id=self.farm_picker_id, component_property="value"),
                Input(component_id=self.kpi_picker_id, component_property="value"),
            ],
        )
        def update_graph(
            selected_start_date,
            selected_end_date,
            selected_farm,
            selected_kpi,
        ):
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
                    dff.groupby(["farm_license", "system_number", "date"])
                    # the table is already aggregated by the columns above,
                    # so it doesn't matter which agg function we use
                    [[selected_kpi]].sum()
                )
                dff.reset_index(inplace=True)

            # Plotly Express
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

        app.run_server(debug=True)


if __name__ == "__main__":
    config_file_path = r"../conf/load_data_from_warehouse.json"
    farm_table_location = r"../spark-warehouse/bronze/bronze_table"
    source_table_location = r"../spark-warehouse/gold/mfr_daily_fact"

    job = VisualizeMfr(
        source_table_location,
        farm_table_location,
        output_container_id="mfr_output_container",
        mapped_id="mfr_daily_fact_map",
        date_picker_id="date_picker",
        farm_picker_id="farm_picker",
        kpi_picker_id="mfr_kpi_picker",
        kpi_list=MFR_KPIS,
        config_file_path=config_file_path,
    )
    job.launch()

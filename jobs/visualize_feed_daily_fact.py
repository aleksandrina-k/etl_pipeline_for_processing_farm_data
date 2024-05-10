#################################################################
# NOT WORKING
#################################################################
from datetime import date, datetime
from jobs.job import Job
from pyspark.sql import functions as F
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, dcc, html, Input, Output

DEFAULT_START_DATE = date(2021, 1, 1)
DEFAULT_END_DATE = date(2023, 12, 31)
KPIS = [
    "loadingAccuracyPerc", "totalRequestedWeightKg",
    "totalLoadedWeightKg", "avgRequestedWeightKg",
    "avgLoadedWeightKg"
]


class VisualizeFeed(Job):

    def __init__(self, config_file_path, spark=None):
        Job.__init__(self, config_file_path, spark)

    def launch(self):
        self.logger.info("Starting Visualization With Job Job")
        config = self.common_initialization()

        # get all farms
        farms = [
            x[0] for x in
            self.spark.read.load("../spark-warehouse/bronze/bronze_table")
            .groupBy("farm_license")
            .count()
            .select("farm_license").collect()
        ]
        farms.sort()

        app = Dash(__name__)

        # ------------------------------------------------------------------------------
        # App layout
        app.layout = html.Div([

            html.H1("Web Application Dashboards with Dash", style={'text-align': 'center'}),

            dcc.DatePickerRange(
                id='date_picker',
                min_date_allowed=DEFAULT_START_DATE,
                max_date_allowed=DEFAULT_END_DATE,
                start_date=DEFAULT_START_DATE,
                end_date=DEFAULT_START_DATE,
                end_date_placeholder_text='Select a date!'
            ),

            dcc.Dropdown(
                id="farm_picker",
                 options=[{"label": x, "value": x} for x in farms],
                 multi=False,
                 value='',
                 style={'width': "40%"}
            ),

            # TODO: Maybe add Dropdown menu for feeds

            dcc.RadioItems(
                id='kpi_picker',
                options=KPIS,
                value=KPIS[0],
            ),


            html.Div(id='output_container', children=[]),
            html.Br(),

            dcc.Graph(id='feed_loading_daily_fact_map', figure={})

        ])

        # -- Import and clean data
        df = (
            self.spark.read
                .load("../spark-warehouse/gold/feed_loading_daily_fact")
                .toPandas()
        )

        # ------------------------------------------------------------------------------
        # Connect the Plotly graphs with Dash Components
        @app.callback(
            [Output(component_id='output_container', component_property='children'),
             Output(component_id='feed_loading_daily_fact_map', component_property='figure')],
            [Input(component_id='date_picker', component_property='start_date'),
             Input(component_id='date_picker', component_property='end_date'),
             Input(component_id='farm_picker', component_property='value'),
             Input(component_id='kpi_picker', component_property='value')]
        )
        def update_graph(option_slctd_start_date, option_slctd_end_date, option_slctd_farm, option_slctd_kpi):

            # container = f"Display {option_slctd_kpi} KPI for {option_slctd_farm} for time period " \
            #             f"[{option_slctd_start_date} - {option_slctd_end_date}]"
            container = ''
            dff = df.copy()

            # in case only one farm is selected
            if option_slctd_farm is not None:
                if isinstance(option_slctd_farm, str):
                    option_slctd_farm = [option_slctd_farm]
                dff = dff[dff["farm_license"].isin(option_slctd_farm)]
            if option_slctd_start_date is not None:
                start_date = datetime.strptime(option_slctd_start_date, '%Y-%m-%d').date()
                dff = dff[dff["date"] >= start_date]
            if option_slctd_end_date is not None:
                end_date = datetime.strptime(option_slctd_end_date, '%Y-%m-%d').date()
                dff = dff[dff["date"] <= end_date]
            if option_slctd_kpi is not None:
                dff = (
                    dff.groupby(['farm_license', 'system_number', 'date', 'feedId'])
                    # the table is already aggregated by the columns above,
                    # so it doesn't matter which agg function we use
                    [[option_slctd_kpi]].sum()
                )
                dff.reset_index(inplace=True)

            # Plotly Express
            fig = px.line(
                data_frame=dff,
                x="date",
                y=option_slctd_kpi,
                color=['feedId'],
                markers=True,
                title=option_slctd_kpi
            )

            return container, fig

        app.run_server(debug=True)


if __name__ == '__main__':
    config_file_path = r"../conf/load_data_from_warehouse.json"

    job = VisualizeFeed(config_file_path)
    job.launch()

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
    FARM_PICKER_ID,
    RATION_KPI_PICKER_ID,
    RATION_CONTAINER_ID,
    RATION_MAPPER_ID,
    RATION_KPIS,
)
from dash_items.callbacks import update_ration_line_chart  # noqa: F401
from operations.helper_functions import extract_all_farm_licenses


class VisualizeRation(Job):
    def __init__(
        self,
        config_file_path: str,
    ):
        Job.__init__(self, config_file_path)
        self.config = self.common_initialization()
        self.farm_list = extract_all_farm_licenses()

    def launch(self):
        self.logger.info("Starting VisualizeRation Job")

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
                    RATION_KPI_PICKER_ID, RATION_KPIS
                ),
                *generate_graph(RATION_CONTAINER_ID, RATION_MAPPER_ID),
            ]
        )

        app.run_server(debug=True)


# if __name__ == "__main__":
#     config_file_path = r"../conf/load_data_from_warehouse.json"
#
#     job = VisualizeRation(config_file_path=config_file_path)
#     job.launch()

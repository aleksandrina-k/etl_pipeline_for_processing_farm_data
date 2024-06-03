from datetime import date
from dash import dcc, html

DEFAULT_START_DATE = date(2023, 1, 1)
DEFAULT_END_DATE = date(2023, 12, 31)


def generate_date_picker_range_component(date_picker_id: str) -> dcc.DatePickerRange:
    return dcc.DatePickerRange(
        id=date_picker_id,
        min_date_allowed=DEFAULT_START_DATE,
        max_date_allowed=DEFAULT_END_DATE,
        start_date=DEFAULT_START_DATE,
        end_date=DEFAULT_START_DATE,
        initial_visible_month=date(2023, 1, 1),
        display_format="YYYY-MM-DD",
        end_date_placeholder_text="Select a date!",
    )


def generate_dropdown_component_with_farms(
    farm_picker_id: str, farms: list[str]
) -> dcc.Dropdown:
    return dcc.Dropdown(
        id=farm_picker_id,
        options=[{"label": x, "value": x} for x in farms],
        multi=True,
        value="",
        style={"width": "40%"},
    )


def generate_radio_button_component_with_kpis(
    kpi_picker_id: str, kpi_list: list[str]
) -> dcc.RadioItems:
    return dcc.RadioItems(id=kpi_picker_id, options=kpi_list, value=kpi_list[0])


def generate_graph(container_id: str, mapper_id):
    return (
        html.Div(id=container_id, children=[]),
        html.Br(),
        dcc.Graph(id=mapper_id, figure={}),
    )

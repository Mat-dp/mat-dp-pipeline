import itertools
import os.path
from pathlib import Path

import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, get_asset_url, html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from mat_dp_pipeline.common import Tree, create_path_tree
from mat_dp_pipeline.pipeline import PipelineOutput

from .graph_colours import GRAPH_COLOURS
from .plotting import (
    indicator_by_resource_agg,
    indicator_by_resource_over_years,
    indicator_by_tech_agg,
    required_resources_agg,
    required_resources_by_tech_agg,
    required_resources_over_years,
)

# the style arguments for the sidebar. We use position:fixed and a fixed width
SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "26rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}

CONTENT_STYLE = {
    "margin-left": "28rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}


def create_dropdown(tree: Tree) -> list[tuple[str, bool]]:
    """Create a padded list of strings out of a tree. Each element, apart from
    the actual text value contains a boolean indicating whether it's a leaf of a tree or not.
    """

    def add_node_to_result(node: Tree, result: list[tuple[str, bool]], prefix: str):
        """
        Adds the specified node to the result list, via a depth first search.
        """
        if node is None:
            return None

        for k, v in node.items():
            new_prefix = f"{(len(prefix))*' '}{k}"
            if v is None:
                result.append((new_prefix, True))
            else:
                result.append((new_prefix, False))
                add_node_to_result(v, result, (len(prefix) + 3) * " ")

    result: list[tuple[str, bool]] = []
    add_node_to_result(tree, result, "")

    return result


class App:
    dash_app: Dash
    outputs: PipelineOutput
    indicators: list[str]
    paths: list[Path]
    tail_labels: list[str]
    main_label: str
    indicator_label: str
    materials: list[str]

    def __init__(
        self,
        outputs: PipelineOutput,
        main_label_override: str | None = None,
        tail_labels_override: list[str] | None = None,
        indicator_label: str = "Emissions",
    ):
        self.dash_app = Dash(
            external_stylesheets=[dbc.themes.BOOTSTRAP],
            assets_folder=str(Path(f"{__file__}").parent / "assets"),
        )
        self.outputs = outputs
        self.indicators = sorted(self.outputs.indicators)
        self.paths = sorted(self.outputs.by_path.keys())

        if tail_labels_override:
            if len(tail_labels_override) > len(outputs.metadata.tail_labels):
                raise ValueError(
                    "Tail labels overrides are longer than the ones from SDF's meta data!"
                )
            self.tail_labels = tail_labels_override
        else:
            self.tail_labels = outputs.metadata.tail_labels

        self.main_label = (
            main_label_override
            if main_label_override is not None
            else outputs.metadata.main_label
        )
        self.indicator_label = indicator_label
        path_inputs = [Input("main", "value")] + [
            Input(f"dropdown_{i}", "value") for i in range(len(self.tail_labels))
        ]
        self.register_callback(
            self.render_tab_content,
            Output("tab-content", "children"),
            Input("tabs", "active_tab"),
            Input("year", "value"),
            path_inputs,
        )

    def _main_dropdown(self) -> dcc.Dropdown:
        """
        * Trim the lowest common ancestor from all the paths' labels (not values!)
        * Trim the lowest levels' bits (as defined by self.tail_labels) - these will
          form other drop downs
        """
        lowest_common_ancestor = os.path.commonpath(self.paths)
        levels_to_trim = len(self.tail_labels)
        if levels_to_trim:
            paths = sorted({Path(*p.parts[:-levels_to_trim]) for p in self.paths})
        else:
            paths = sorted(self.paths)

        # combine paths as values and labels from a created tree. For non-leaf elements
        # in the tree, there's no value.
        paths_without_lca = [p.relative_to(lowest_common_ancestor) for p in paths]
        p_idx = 0
        dropdown_items = create_dropdown(create_path_tree(paths_without_lca))
        options = []
        for label, is_leaf in dropdown_items:
            if is_leaf:
                value = str(paths[p_idx])
                p_idx += 1
            else:
                value = None
            options.append(
                {
                    "label": label.replace(" ", "\u2000"),
                    "value": value,
                    "disabled": not is_leaf,
                }
            )

        return dcc.Dropdown(options=options, id="main")

    def _leaf_dropdowns(self) -> list[tuple[str, dcc.Dropdown]]:
        levels_to_trim = len(self.tail_labels)
        dropdowns = []
        for i, category in enumerate(self.tail_labels):
            # Take only a set of parts of the paths on the lower levels,
            # starting from the highest one, moving towards to the leaf
            paths = sorted(
                {p.parts[len(p.parts) - (levels_to_trim - i)] for p in self.paths}
            )
            dropdowns.append((category, dcc.Dropdown(paths, id=f"dropdown_{i}")))
        return dropdowns

    def layout(self):
        leaf_dropdowns = [
            html.Div([html.Label(label), dd]) for label, dd in self._leaf_dropdowns()
        ]
        year_options = ["All"] + [str(y) for y in self.outputs.by_year.keys()]
        year_dropdown = dcc.Dropdown(options=year_options, id="year", value="All")

        sidebar = html.Div(
            [
                html.Img(src=get_asset_url("MatDP.png"), width=128),
                html.Hr(),
                html.P("Choose appropriate options", className="lead"),
                html.Hr(),
                html.Div([html.Label(self.main_label), self._main_dropdown()]),
                *leaf_dropdowns,
                html.Div([html.Label("Year"), year_dropdown]),
                html.Center(
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Img(
                                        src=get_asset_url("cambridge_logo.png"),
                                        width=128,
                                    ),
                                    html.Img(
                                        src=get_asset_url("Refficiency.png"), width=128
                                    ),
                                ]
                            ),
                            html.Img(src=get_asset_url("CCG_logo.png"), width=128),
                        ]
                    )
                ),
            ],
            style=SIDEBAR_STYLE,
        )

        tabs = [dbc.Tab(label="Materials", tab_id="materials")]
        for i, ind in enumerate(self.indicators):
            tabs.append(dbc.Tab(label=ind, tab_id=f"ind_{i}"))

        content = html.Div(
            [
                dcc.Store(id="store"),
                html.H1("Mat-dp results"),
                html.Hr(),
                dbc.Tabs(tabs, id="tabs", active_tab="materials"),
                html.Div(id="tab-content", className="p-4"),
            ],
            style=CONTENT_STYLE,
        )

        return html.Div([sidebar, content])

    def render_tab_content(self, active_tab: str, year: str, *path_parts):
        # Path has missing levels - do not update
        if any(not p for p in path_parts):
            raise PreventUpdate

        year_i = None if year == "All" else int(year)

        path = Path(*path_parts)
        is_indicator_tab = active_tab.startswith("ind_")
        if is_indicator_tab:
            ind_idx = int(active_tab.split("_")[1])
            indicator = self.indicators[ind_idx]
            plots = self.generate_indicator_graphs(path, indicator, year_i)
        else:  # materials tab
            plots = self.generate_materials_graphs(path, year_i)

        return [dcc.Graph(figure=fig, style={"height": "25vh"}) for fig in plots]

    def get_color_map(self, path):
        materials = self.outputs.resources(path).sum()
        materials = materials[materials > 0]
        scheme_iter = itertools.cycle(GRAPH_COLOURS)
        return {mat: next(scheme_iter) for mat in materials.index}

    def generate_materials_graphs(
        self, path: Path, year: int | None
    ) -> list[go.Figure]:
        color_map = self.get_color_map(path)
        return [
            required_resources_over_years(self.outputs, path, color_map),
            required_resources_by_tech_agg(self.outputs, path, year, color_map),
            required_resources_agg(self.outputs, path, year, color_map),
        ]

    def generate_indicator_graphs(
        self, path: Path, indicator: str, year: int | None
    ) -> list[go.Figure]:
        color_map = self.get_color_map(path)
        return [
            indicator_by_resource_over_years(
                self.outputs, path, indicator, color_map, self.indicator_label
            ),
            indicator_by_tech_agg(
                self.outputs, path, indicator, year, color_map, self.indicator_label
            ),
            indicator_by_resource_agg(
                self.outputs,
                path,
                indicator,
                year,
                color_map,
                self.indicator_label,
            ),
        ]

    def register_callback(self, fn, *spec):
        self.dash_app.callback(*spec)(fn)

    def serve(self):
        self.dash_app.layout = self.layout()
        self.dash_app.run_server(debug=False)

from pyspark.sql import SparkSession
from pyvis.network import Network
import networkx as nx
from layer_mappings.gold_mapping import gold_mapping
from layer_mappings.silver1_mapping import silver1_mapping


def illustrate_mapping():
    # get tables from all layers
    silver1 = silver1_mapping()
    gold = gold_mapping()
    silver0_tables = filter(
        lambda table: any(table in conf.input_tables for conf in silver1.confs),
        silver1.existing_tables,
    )
    silver1_tables = silver1.mapping.keys()
    gold_tables = gold.mapping.keys()

    # build the graph
    nx_graph = nx.DiGraph()
    nx_graph.add_nodes_from(
        silver0_tables, group=1, color="lightblue", level=1, shape="box", physics=True
    )
    nx_graph.add_nodes_from(
        silver1_tables,
        group=2,
        color="silver",
        level=2,
        shape="box",
        physics=True,
    )
    nx_graph.add_nodes_from(
        gold_tables, group=3, color="gold", level=3, shape="box", physics=True
    )

    for conf in silver1.confs:
        for it in conf.input_tables:
            if not nx_graph.nodes.get(it):
                raise Exception(
                    f"There is no input table {it} to build table {conf.result_table}"
                )
            nx_graph.add_edge(
                it,
                conf.result_table,
                length=300,
                color={"color": "silver", "highlight": "black"},
            )

    for table_name, conf in gold.mapping.items():
        for it in conf.input_tables:
            # fix: get input tables from silver0
            # mfr_indications_daily_count_transformer input tables
            # are from silver0 and silver1
            if not nx_graph.nodes.get(it):
                if it in silver1.existing_tables:
                    nx_graph.add_node(
                        it,
                        group=1,
                        color="lightblue",
                        level=1,
                        shape="box",
                        physics=True,
                    )
            nx_graph.add_edge(
                it,
                table_name,
                length=300,
                color={"color": "gold", "highlight": "black"},
            )

    # set hierarchy levels (topological sort + push gold further down)
    generations = nx.topological_generations(nx_graph)
    all_generations = list(generations)
    group = 1
    gold_level = 1
    STANDARD_SIZE = 15
    for g in all_generations:
        for node in g:
            if nx_graph.nodes.get(node)["color"] == "gold":
                nx_graph.nodes.get(node)["group"] = len(all_generations) + gold_level
                nx_graph.nodes.get(node)["level"] = len(all_generations) + gold_level
                nx_graph.nodes.get(node)["size"] = (
                    len(all_generations) + gold_level
                ) * 5 + STANDARD_SIZE
            else:
                nx_graph.nodes.get(node)["group"] = group
                nx_graph.nodes.get(node)["level"] = group
                nx_graph.nodes.get(node)["size"] = group * 5 + STANDARD_SIZE
        group = group + 1
        gold_level += 1

    # populates the nodes and edges data structures
    net = Network("1100px", "100%", directed=True, layout="hierarchical")

    # Post Process Labels
    net.options.physics.use_hrepulsion(
        {
            "node_distance": 300,
            "central_gravity": 0,
            "spring_length": 100,
            "spring_strength": 0.01,
            "damping": 0.09,
        }
    )
    # create network
    net.from_nx(nx_graph)
    neighbor_map = net.get_adj_list()
    # add neighbor data to node hover data
    for node in net.nodes:
        string = "<b>Uses:</b><br>" + "<br>".join(
            [k for k, v in neighbor_map.items() if node["id"] in v]
        )
        string += "<br><b>Used By:</b><br>" + "<br>".join(
            neighbor_map[node["id"]]
        )
        node["title"] = string

    net.show_buttons()
    net.show(name="table_dependencies_illustration.html", notebook=False)


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    illustrate_mapping()

from typing import Iterator, List, Set
import networkx as nx
from exceptions import (
    DuplicatesTablesException,
    NotAllInputTablesAreDeclaredException,
    UnknownInputTableException,
    CyclicDependenciesException,
)
from orchestration.trans_conf import TransConf


class TransMapping:
    """
    Transformation mapping
    Setups mapping and provides an order in which transformation have to be executed
    """

    def __init__(
        self, confs: List[TransConf], existing_tables: Set[str]  # silver0 tables
    ):
        self.confs = confs
        self.existing_tables = existing_tables

        # check for duplicates
        TransMapping._check_for_duplicates(confs)

        # build mapping
        self.mapping = {conf.result_table: conf for conf in confs}

        # validate that all tables exist
        all_declared_tables = set(self.existing_tables) | self.mapping.keys()
        all_input_tables = set(it for conf in confs for it in conf.input_tables)

        not_declared_tables = all_input_tables - all_declared_tables
        if not_declared_tables:
            raise NotAllInputTablesAreDeclaredException(not_declared_tables)

        # build graph
        self.graph = TransMapping._build_graph(self.mapping, self.existing_tables)

        # validate graph
        TransMapping._validate_graph(self.graph)

    def get_all_tables(self) -> Set[str]:
        return self.mapping.keys() | self.existing_tables

    def _get_tables_to_process(self) -> Set[str]:
        all_tables = [t for t, v in self.mapping.items()]
        return set(all_tables)

    def transformations_in_order(self) -> Iterator[TransConf]:
        tables_to_process = self._get_tables_to_process()
        for generation in nx.topological_generations(self.graph):
            for result_table in generation:
                if result_table in tables_to_process:
                    yield self.mapping[result_table]

    @staticmethod
    def _check_for_duplicates(confs: List[TransConf]):
        result_tables = [conf.result_table for conf in confs]

        duplicates = set(
            result_table
            for result_table in result_tables
            if result_tables.count(result_table) > 1
        )

        if duplicates:
            raise DuplicatesTablesException(duplicates)

    @staticmethod
    def _build_graph(mapping: dict, existing_tables: Set[str]) -> nx.DiGraph:
        nx_graph = nx.DiGraph()
        nx_graph.add_nodes_from(existing_tables, group=1)
        nx_graph.add_nodes_from(mapping.keys(), group=2)

        for result_table, conf in mapping.items():
            for it in conf.input_tables:
                if not nx_graph.nodes.get(it):
                    raise UnknownInputTableException(it, result_table)
                nx_graph.add_edge(it, result_table)

        return nx_graph

    @staticmethod
    def _validate_graph(nx_graph: nx.DiGraph):
        try:
            cycles = nx.find_cycle(nx_graph, orientation="original")
            raise CyclicDependenciesException(cycles)
        except nx.NetworkXNoCycle:
            pass

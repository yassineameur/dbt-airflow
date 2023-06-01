import os
from os import path, environ

from typing import List, Dict, Any, Tuple, Optional
import json
from enum import Enum
import yaml

from jinja2 import Environment, FileSystemLoader

DATABASE_NAME = "wine_services_dbt"

SOURCE_TESTS_GROUP_NAME = "sources_tests"


class TaskType(Enum):
    TEST = "test"
    RUN = "run"


def load_manifest_file():
    with open("./manifest.json") as f:
        data = json.load(f)

    return data


def is_model(node_name: str) -> bool:
    return node_name.split(".")[0] == "model"


class Node:
    def __init__(self, node_name, is_source: bool = False, has_tests: bool = True):
        self.node_name = node_name
        self.is_source = is_source
        self.has_tests = has_tests

    def __eq__(self, other):
        return isinstance(other, Node) and other.node_name == self.node_name and other.is_source == self.is_source

    @property
    def model_name(self):
        if self.is_source is True:
            return self.node_name

        return self.node_name.split(".")[-1]

    @property
    def run_task_id(self) -> str:
        return self.node_name.replace("model", TaskType.RUN.value) \
            .replace(".", "_").replace(DATABASE_NAME + "_", "")

    @property
    def test_task_id(self) -> str:
        return self.node_name.replace("model", TaskType.TEST.value). \
            replace(".", "_").replace(DATABASE_NAME + "_", "").replace("source:", "")

    def _build_airflow_task_expression(self, task_type: TaskType) -> str:
        task_id = self.test_task_id if task_type == TaskType.TEST else self.run_task_id
        pool = "dbt_run" if task_type == TaskType.RUN else "dbt_test"
        return f"""{task_id} = ExecuteDBTJob(task_id="{task_id}", test_or_run="{task_type.value}", model_name="{self.model_name}", trigger_rule="none_failed", pool="{pool}")"""

    @property
    def task_group_id(self) -> str:
        return self.node_name.replace(".", "_").replace(DATABASE_NAME + "_", "")

    def get_run_task_expression(self) -> str:
        return self._build_airflow_task_expression(task_type=TaskType.RUN)

    def get_test_task_expression(self) -> str:
        return self._build_airflow_task_expression(task_type=TaskType.TEST)

    def get_tasks_group_expression(self):
        group_expression = f"""with TaskGroup(group_id="{self.task_group_id}") as {self.task_group_id}:
        {self.get_run_task_expression()}"""
        if self.has_tests:
            group_expression += f"""
        {self.get_test_task_expression()}
        {self.run_task_id} >> {self.test_task_id}
        """
        return group_expression


class Graph:
    def __init__(
            self,
            selector_name: str,
            selector_schedule: Optional[str],
            models_nodes: List[Node],
            nodes_dependencies: List[Tuple[Node, Node]],
            sources_nodes: List[Node]
    ):
        self.name = "dbt_" + selector_name
        self.models_nodes = models_nodes
        self.sources_nodes = sources_nodes

        self.schedule = None if selector_schedule is None else f"\"{selector_schedule}\""
        assert len(models_nodes) > 1, f"No nodes for graph {self.name}"
        self.nodes_dependencies = nodes_dependencies
        self.validate_models_dependencies()

    def validate_models_dependencies(self):
        """
        Make sure that all nodes dependencies are in nodes list
        """
        nodes_names = {node.node_name for node in self.models_nodes}
        for (start_node, target_node) in self.nodes_dependencies:
            assert start_node.node_name in nodes_names, f"Unknown node {start_node.node_name}"
            assert target_node.node_name in nodes_names, f"Unknown node {target_node.node_name}"

    def build_dbt_test_sources_task(self):

        task_group_expression = f"""with TaskGroup(group_id="{SOURCE_TESTS_GROUP_NAME}") as {SOURCE_TESTS_GROUP_NAME}:"""
        tests_tasks_expressions = "\n        ".join([source_node.get_test_task_expression() for source_node in self.sources_nodes])
        return task_group_expression + "\n        " + tests_tasks_expressions

    def build_dbt_tasks_list_expressions(self) -> List[str]:
        """
        :return: dbt tasks expression
        """
        dbt_tasks_expressions_list = []
        # add test sources task group
        if self.sources_nodes:
            dbt_tasks_expressions_list.append(self.build_dbt_test_sources_task())

        for node in self.models_nodes:
            dbt_tasks_expressions_list.append(node.get_tasks_group_expression())

        return dbt_tasks_expressions_list

    def _get_root_nodes(self) -> List[Node]:
        """List of models without parents"""
        nodes_with_parents = []
        for dependency in self.nodes_dependencies:
            child = dependency[1]
            nodes_with_parents.append(child)

        root_nodes = [node for node in self.models_nodes if node not in nodes_with_parents]

        return root_nodes

    def build_tasks_dependencies_expressions(self):
        dependencies_expressions = []
        for (start_node, end_node) in self.nodes_dependencies:
            dependencies_expressions.append(f"{start_node.task_group_id} >> {end_node.task_group_id}")

        if self.sources_nodes:
            roots_nodes = self._get_root_nodes()

            # build a dependency between tests_source and each root node
            for root_node in roots_nodes:
                dependencies_expressions.append(f"{SOURCE_TESTS_GROUP_NAME} >> {root_node.task_group_id}")

        return dependencies_expressions

    def build(self):
        self.tasks_list_expressions = self.build_dbt_tasks_list_expressions()
        self.tasks_dependencies_expressions = self.build_tasks_dependencies_expressions()


def get_nodes_dependencies(nodes: Dict[str, Node], manifest_graph: Dict[str, Any]) -> List[Tuple[Node, Node]]:
    """
    :return: dependencies expressions between each consecutive nodes
    """

    nodes_dependencies = []
    for node_name, node in nodes.items():
        upstream_nodes_names = {
            node_name for node_name in manifest_graph["nodes"][node_name]["depends_on"]["nodes"]
            if is_model(node_name)
        }

        for upstream_nodes_name in upstream_nodes_names:
            upstream_node = Node(node_name=upstream_nodes_name)
            nodes_dependencies.append((upstream_node, node))

    return nodes_dependencies


def get_jinja_template():
    current_dir = path.dirname(path.realpath(__file__))
    env = Environment(loader=FileSystemLoader(current_dir))
    return env.get_template('dag_template.txt')


def load_selectors_names():
    selectors_path = path.join(environ["DBT_PROFILES_DIR"], "selectors.yml")
    with open(selectors_path) as f:
        dag_model_selectors = yaml.full_load(f)
        for selector in dag_model_selectors["selectors"]:

            yield selector["name"], selector["definition"].get("schedule")


def parse_model_selector(selector_name):
    """Run the dbt ls command which returns all dbt models associated with a particular
    selection syntax"""

    dbt_dir = os.environ["DBT_PROFILES_DIR"]

    models = os.popen(f"cd {dbt_dir} && dbt ls --selector {selector_name}").read()

    models = models.splitlines()
    # rework models names to dapt it to manifest.json manner

    models_names = []
    sources_names = []
    for model in models:

        source_prefix = f"source:{DATABASE_NAME}."
        if model.startswith(source_prefix):
            source_name = model.replace(source_prefix, "source:")
            sources_names.append(source_name)
        else:
            table_name = model.split(".")[-1]
            db_name = model.split(".")[0]
            model_name = f"model.{db_name}.{table_name}"
            models_names.append(model_name)

    return models_names, sources_names


def build_dag_for_selector(
        selector_name: str,
        selector_schedule: Optional[str],
        all_nodes: List[Node],
        all_nodes_dependencies: List[Tuple[Node, Node]]
) -> Graph:
    print(f"Building dag for selector {selector_name} ...")
    nodes_in_selector, sources_in_selector = parse_model_selector(selector_name=selector_name)

    all_models_names = {node.node_name for node in all_nodes}

    # take only models in manifest.json
    dag_nodes_names = set(nodes_in_selector).intersection(all_models_names)
    dag_nodes = [Node(node_name, is_source=False, has_tests=False) for node_name in dag_nodes_names]
    print(f"Dag nodes are got: {len(dag_nodes)} node")

    sources_nodes = [Node(source_name, is_source=True, has_tests=False) for source_name in sources_in_selector]
    # filter source nodes: take only the ones where there are tests

    # all tests names
    tests_nodes_names = set(nodes_in_selector) - all_models_names

    # if node is mentioned in tests => mark it as has tests
    for node in dag_nodes:
        node_name = node.model_name
        for test in tests_nodes_names:
            if node_name in test:
                node.has_tests = True
                break

    # take only sources that are mentioned in tests
    sources_nodes_with_tests = []
    for source_node in sources_nodes:
        node_name = source_node.node_name.replace("source:", "")
        source_test_name = node_name.split(".")[0] + "_" + node_name.split(".")[1]
        for test_node_name in tests_nodes_names:
            if source_test_name in test_node_name:
                sources_nodes_with_tests.append(source_node)
                break

    print(f"Found {len(dag_nodes)} nodes and {len(sources_nodes)} sources for selector {selector_name}")

    dag_nodes_dependencies = [
        (start_node, end_node)
        for (start_node, end_node) in all_nodes_dependencies
        if start_node.node_name in dag_nodes_names and end_node.node_name in dag_nodes_names
    ]
    print(f"Found {len(dag_nodes_dependencies)} dependencies for selector {selector_name}")

    graph = Graph(
        selector_name=selector_name,
        selector_schedule=selector_schedule,
        models_nodes=dag_nodes,
        nodes_dependencies=dag_nodes_dependencies,
        sources_nodes=sources_nodes_with_tests
    )
    print(f"Building graph for selector {selector_name}")
    graph.build()
    return graph


def run():
    manifest_graph = load_manifest_file()

    all_dbt_nodes = {
        manifest_node_name: Node(manifest_node_name) for manifest_node_name in manifest_graph["nodes"].keys()
        if is_model(manifest_node_name)
    }
    all_nodes_dependencies = get_nodes_dependencies(nodes=all_dbt_nodes, manifest_graph=manifest_graph)

    dag_model_selectors = load_selectors_names()

    dags = []
    for selector_name, selector_schedule in dag_model_selectors:

        dag = build_dag_for_selector(
            selector_name=selector_name,
            selector_schedule=selector_schedule,
            all_nodes=list(all_dbt_nodes.values()),
            all_nodes_dependencies=all_nodes_dependencies
        )
        dags.append(dag)

    template = get_jinja_template()
    dag_expression = template.render(dags=dags)

    with open("dbt_dag.py", "w") as f:
        f.write(dag_expression)


if __name__ == "__main__":
    run()

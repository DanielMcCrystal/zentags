from collections import defaultdict
import itertools
import networkx as nx


class DAG(nx.DiGraph):
    def add_edge(self, u_of_edge, v_of_edge, **attr):
        if (self.has_node(u_of_edge) and self.has_node(v_of_edge)) and (
            self.has_edge(v_of_edge, u_of_edge)
            or nx.has_path(self, v_of_edge, u_of_edge)
        ):
            raise ValueError("Adding this edge will create a cycle.")
        return super().add_edge(u_of_edge, v_of_edge, **attr)


def recursive_defaultdict():
    return defaultdict(recursive_defaultdict)


def convert_to_normal_dict(d):
    if isinstance(d, defaultdict):
        d = {k: convert_to_normal_dict(v) for k, v in d.items()}
    return d


def recursive_dict_traversal(d):
    for k, v in d.items():
        yield k
        yield from recursive_dict_traversal(v)


class FileSystem:
    def __init__(self) -> None:
        self.tagged: DAG = DAG()

    def load(self, source_file: str):
        with open(source_file) as f:
            for line in f:
                self.__parse_line(line)

    def __ingest_container_path(self, path: str) -> None:
        items = path.split("/")
        last_container = None
        for item in itertools.accumulate(items, lambda x, y: f"{x}/{y}"):
            if last_container is not None:
                self.tagged.add_edge(last_container, item)
            last_container = item

    def __parse_line(self, line: str):
        line = line.strip()
        if line.startswith("#") or line == "":
            return

        items = line.split(" ")
        source = items[0]
        self.__ingest_container_path(source)

        tags = items[1:]
        for tag in tags:
            self.__ingest_container_path(tag)
            self.tagged.add_edge(tag, source)

    def get_item_is_tagged_by(self, item: str, tag: str) -> bool:
        return nx.has_path(self.tagged, tag, item)

    def get_leaf_nodes_tagged_by(self, tags: list[str]) -> set[str]:
        leaf_nodes = {
            node
            for node in nx.descendants(self.tagged, tags[0])
            if self.tagged.out_degree(node) == 0
        }
        for tag in tags[1:]:
            leaf_nodes.intersection_update(
                {
                    node
                    for node in nx.descendants(self.tagged, tag)
                    if self.tagged.out_degree(node) == 0
                }
            )
        return leaf_nodes

    def get_items_immediately_tagged_by(self, tags: list[str]) -> set[str]:
        items = set(self.tagged.successors(tags[0]))
        for tag in tags[1:]:
            items.intersection_update(set(self.tagged.successors(tag)))
        return items

    def get_items_contained_by(self, container_key: str) -> set[str]:
        return set(
            item
            for item in self.get_items_immediately_tagged_by([container_key])
            if item.startswith(f"{container_key}/")
        )

    def get_root_tags(self) -> set[str]:
        return {node for node in self.tagged.nodes if self.tagged.in_degree(node) == 0}

    def get_dot(self):
        return nx.nx_agraph.to_agraph(self.tagged)

    def get_tags(self, item: str) -> set[str]:
        return set(self.tagged.predecessors(item))


class FileBrowser:
    def __init__(self, fs: FileSystem) -> None:
        self.fs = fs
        self.applied_tags = set()
        self.useful_tags = self.fs.get_root_tags()

    def get_useful_tags(self) -> set[str]:
        return self.useful_tags

    def add_tag(self, tag: str):
        self.applied_tags.add(tag)
        file_results = self.submit_query()

        if tag in self.useful_tags:
            self.useful_tags.remove(tag)

        self.useful_tags.update(self.fs.get_items_immediately_tagged_by([tag]))

        new_useful_tags = set()
        for useful_tag in self.useful_tags:
            new_query_result = self.fs.get_leaf_nodes_tagged_by(
                [*self.applied_tags, useful_tag]
            )
            if new_query_result.issubset(file_results) and len(new_query_result) > 0:
                new_useful_tags.add(useful_tag)
        self.useful_tags = new_useful_tags

    def submit_query(self) -> set[str]:
        if len(self.applied_tags) == 0:
            return {}
        return self.fs.get_leaf_nodes_tagged_by(list(self.applied_tags))


class Shell:
    def __init__(self, fs: FileSystem):
        self.browser = FileBrowser(fs)

    def format_taglist(self, tags: set[str]) -> str:
        return " ".join(f"[{tag}]" for tag in sorted(tags))

    def ls_files(self):
        for file in self.browser.submit_query():
            print(file, self.format_taglist(self.browser.fs.get_tags(file)))

    def ls_tags(self):
        for tag in self.browser.applied_tags:
            print(tag, self.format_taglist(self.browser.fs.get_tags(tag)))

    def refine(self):
        for tag in self.browser.get_useful_tags():
            print(tag, self.format_taglist(self.browser.fs.get_tags(tag)))

    def add_tag(self, tag: str):
        self.browser.add_tag(tag)
        self.ls_tags()

    def start(self):
        while True:
            command = input("> ")
            if command == "ls":
                self.ls_files()
            elif command == "tags":
                self.ls_tags()
            elif command == "ref":
                self.refine()
            elif command.startswith("add "):
                self.add_tag(command.split(" ")[1])
            elif command == "exit":
                break
            else:
                print("Invalid command")
            print()


fs = FileSystem()
fs.load("tags.txt")

shell = Shell(fs)
shell.start()

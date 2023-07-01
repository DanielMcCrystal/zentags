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

    def get_leaf_nodes_tagged_by(self, tags: list[str]) -> set[str]:
        # Get items explicitly tagged by this tag
        return {node for tag in tags for node in nx.descendants(self.tagged, tag) if self.tagged.out_degree(node) == 0}
    
    def get_items_immediately_tagged_by(self, tags: list[str]) -> set[str]:
        items = set(self.tagged.successors(tags[0]))
        for tag in tags[1:]:
            items.intersection_update(set(self.tagged.successors(tag)))
        return items

    def get_items_contained_by(self, container_key: str) -> set[str]:
        return set(item for item in self.get_items_immediately_tagged_by([container_key]) if item.startswith(f"{container_key}/"))

    def get_root_tags(self) -> set[str]:
        return {node for node in self.tagged.nodes if self.tagged.in_degree(node) == 0}

    def get_dot(self):
        return nx.nx_agraph.to_agraph(self.tagged)

class FileBrowser:
    def __init__(self, fs: FileSystem) -> None:
        self.fs = fs
        self.tags = set()

    def get_potential_tags(self):
        if len(self.tags) == 0:
            return self.fs.get_root_tags()
        else:
            return self.fs.get_items_immediately_tagged_by(list(self.tags))
        
    def add_tag(self, tag: str):
        self.tags.add(tag)

    def submit_query(self) -> set[str]:
        if len(self.tags) == 0:
            raise ValueError("Must provide at least one tag")
        return self.fs.get_leaf_nodes_tagged_by(self.tags)

fs = FileSystem()
fs.load("tags.txt")

fb = FileBrowser(fs)

print(fb.get_potential_tags())
fb.add_tag("documents")
print(fb.get_potential_tags())

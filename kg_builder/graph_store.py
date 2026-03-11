import json
from pathlib import Path


class GraphStore:
    def __init__(self, out_dir: str):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self.nodes_path = self.out_dir / "nodes.jsonl"
        self.edges_path = self.out_dir / "edges.jsonl"

        self._seen_nodes = set()
        self._seen_edges = set()

        # start fresh
        self.nodes_path.write_text("", encoding="utf-8")
        self.edges_path.write_text("", encoding="utf-8")

        self.node_count = 0
        self.edge_count = 0

    def add_node(self, node_id: str, label: str, **props):
        key = (node_id, label)
        if key in self._seen_nodes:
            return
        self._seen_nodes.add(key)

        rec = {
            "id": node_id,
            "label": label,
            "properties": props,
        }

        with open(self.nodes_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        self.node_count += 1

    def add_edge(self, src: str, rel: str, dst: str, **props):
        key = (src, rel, dst, json.dumps(props, sort_keys=True))
        if key in self._seen_edges:
            return
        self._seen_edges.add(key)

        rec = {
            "src": src,
            "rel": rel,
            "dst": dst,
            "properties": props,
        }

        with open(self.edges_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        self.edge_count += 1

    def write_jsonl(self, filename: str, records):
        path = self.out_dir / filename
        with open(path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
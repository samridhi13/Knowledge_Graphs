import argparse
import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path

from tqdm import tqdm

from kg_builder.graph_store import GraphStore
from kg_builder.pubmed_client import fetch_pubmed_records, search_pmids
from kg_builder.umls_linker import UMLSLinkerWrapper
from kg_builder.umls_rrf import iter_mrrel_edges, load_mrsty_map


def author_id(name: str) -> str:
    return "author:" + hashlib.md5(name.strip().lower().encode("utf-8")).hexdigest()[:16]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--max_papers", type=int, default=500)
    parser.add_argument("--out_dir", type=str, required=True)
    parser.add_argument("--min_link_score", type=float, default=0.85)
    parser.add_argument("--model_name", type=str, default="en_core_sci_sm")
    parser.add_argument("--umls_mrrel", type=str, default=None)
    parser.add_argument("--umls_mrsty", type=str, default=None)
    parser.add_argument("--umls_mode", type=str, default="closed", choices=["closed", "onehop"])
    parser.add_argument("--umls_neighbor_cap", type=int, default=50)
    args = parser.parse_args()

    email = os.environ.get("NCBI_EMAIL", "").strip()
    api_key = os.environ.get("NCBI_API_KEY", "").strip() or None

    if not email:
        raise RuntimeError("Please set NCBI_EMAIL in your environment before running.")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Searching PubMed...")
    pmids = search_pmids(args.query, args.max_papers, email, api_key)
    print(f"Found {len(pmids)} PMIDs")

    print("Fetching PubMed records...")
    records = fetch_pubmed_records(pmids, email, api_key)

    print("Loading UMLS linker...")
    linker = UMLSLinkerWrapper(model_name=args.model_name, threshold=args.min_link_score)

    graph = GraphStore(args.out_dir)

    paper_meta_records = []
    paper_cuis_records = []

    corpus_cuis = set()
    concept_name_map = {}

    print("Building Paper / Author / Concept graph...")
    for rec in tqdm(records):
        pmid = rec["pmid"]
        paper_node = f"paper:{pmid}"

        graph.add_node(
            paper_node,
            "Paper",
            pmid=pmid,
            title=rec["title"],
            abstract=rec["abstract"],
            year=rec["year"],
            journal=rec["journal"],
        )

        for a in rec["authors"]:
            aid = author_id(a)
            graph.add_node(aid, "Author", name=a)
            graph.add_edge(paper_node, "HAS_AUTHOR", aid)

        links = []
        links.extend(linker.link_text(rec["title"], section="title"))
        links.extend(linker.link_text(rec["abstract"], section="abstract"))

        # aggregate by CUI for this paper
        per_cui = defaultdict(lambda: {"max_score": 0.0, "mentions": [], "sections": set()})
        for x in links:
            cui = x["cui"]
            per_cui[cui]["max_score"] = max(per_cui[cui]["max_score"], x["score"])
            per_cui[cui]["mentions"].append(x["mention"])
            per_cui[cui]["sections"].add(x["section"])

            if x.get("canonical_name"):
                concept_name_map[cui] = x["canonical_name"]

        paper_cuis = []

        for cui, info in per_cui.items():
            paper_cuis.append(cui)
            corpus_cuis.add(cui)

            concept_node = f"cui:{cui}"
            graph.add_node(
                concept_node,
                "Concept",
                cui=cui,
                canonical_name=concept_name_map.get(cui),
            )

            graph.add_edge(
                paper_node,
                "MENTIONS",
                concept_node,
                score=info["max_score"],
                mention_count=len(info["mentions"]),
                sections=sorted(list(info["sections"])),
            )

        paper_meta_records.append(
            {
                "pmid": pmid,
                "title": rec["title"],
                "year": rec["year"],
                "journal": rec["journal"],
                "authors": rec["authors"],
            }
        )

        paper_cuis_records.append(
            {
                "pmid": pmid,
                "cuis": sorted(paper_cuis),
            }
        )

    # semantic types
    mrsty_count = 0
    if args.umls_mrsty:
        print("Loading MRSTY and adding semantic types...")
        mrsty_map = load_mrsty_map(args.umls_mrsty, allowed_cuis=corpus_cuis)

        for cui, entries in tqdm(mrsty_map.items()):
            concept_node = f"cui:{cui}"
            for e in entries:
                tui = e["tui"]
                sty = e["sty"]
                tui_node = f"tui:{tui}"

                graph.add_node(tui_node, "SemanticType", tui=tui, name=sty)
                graph.add_edge(concept_node, "HAS_SEMANTIC_TYPE", tui_node, sty=sty)
                mrsty_count += 1

    # UMLS concept-concept relations
    mrrel_count = 0
    if args.umls_mrrel:
        print("Loading MRREL and adding UMLS relations...")
        for e in tqdm(
            iter_mrrel_edges(
                args.umls_mrrel,
                allowed_cuis=corpus_cuis,
                mode=args.umls_mode,
                neighbor_cap=args.umls_neighbor_cap,
            )
        ):
            src = f"cui:{e['cui1']}"
            dst = f"cui:{e['cui2']}"
            graph.add_node(src, "Concept", cui=e["cui1"], canonical_name=concept_name_map.get(e["cui1"]))
            graph.add_node(dst, "Concept", cui=e["cui2"], canonical_name=concept_name_map.get(e["cui2"]))
            graph.add_edge(
                src,
                "UMLS_REL",
                dst,
                rel=e["rel"],
                rela=e["rela"],
                sab=e["sab"],
            )
            mrrel_count += 1

    graph.write_jsonl("paper_meta.jsonl", paper_meta_records)
    graph.write_jsonl("paper_cuis.jsonl", paper_cuis_records)

    stats = {
        "query": args.query,
        "max_papers_requested": args.max_papers,
        "papers_fetched": len(records),
        "unique_cuis": len(corpus_cuis),
        "nodes_written": graph.node_count,
        "edges_written": graph.edge_count,
        "semantic_type_edges": mrsty_count,
        "umls_relation_edges": mrrel_count,
    }

    with open(out_dir / "stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print("\nDone.")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
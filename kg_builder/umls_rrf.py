from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set


def load_mrsty_map(mrsty_path: str, allowed_cuis: Optional[Set[str]] = None) -> Dict[str, List[dict]]:
    """
    MRSTY columns:
    CUI|TUI|STN|STY|ATUI|CVF|
    """
    out = defaultdict(list)

    with open(mrsty_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.rstrip("\n").split("|")
            if len(parts) < 4:
                continue

            cui = parts[0]
            tui = parts[1]
            sty = parts[3]

            if allowed_cuis is not None and cui not in allowed_cuis:
                continue

            out[cui].append({"tui": tui, "sty": sty})

    return dict(out)


def iter_mrrel_edges(
    mrrel_path: str,
    allowed_cuis: Optional[Set[str]] = None,
    mode: str = "closed",
    neighbor_cap: int = 50,
) -> Iterable[dict]:
    """
    MRREL columns:
    CUI1|AUI1|STYPE1|REL|CUI2|AUI2|STYPE2|RELA|RUI|SRUI|SAB|SL|RG|DIR|SUPPRESS|CVF|

    mode='closed': both CUI1 and CUI2 must already be in allowed_cuis
    mode='onehop': at least one endpoint is in allowed_cuis
    """
    if mode not in {"closed", "onehop"}:
        raise ValueError("mode must be 'closed' or 'onehop'")

    source_count = defaultdict(int)

    with open(mrrel_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.rstrip("\n").split("|")
            if len(parts) < 15:
                continue

            cui1 = parts[0]
            rel = parts[3]
            cui2 = parts[4]
            rela = parts[7]
            sab = parts[10]
            suppress = parts[14] if len(parts) > 14 else ""

            if suppress == "Y":
                continue

            if not cui1 or not cui2 or cui1 == cui2:
                continue

            if allowed_cuis is not None:
                in1 = cui1 in allowed_cuis
                in2 = cui2 in allowed_cuis

                if mode == "closed" and not (in1 and in2):
                    continue
                if mode == "onehop" and not (in1 or in2):
                    continue

                if mode == "onehop":
                    anchor = cui1 if in1 else cui2
                    if source_count[anchor] >= neighbor_cap:
                        continue
                    source_count[anchor] += 1

            yield {
                "cui1": cui1,
                "cui2": cui2,
                "rel": rel,
                "rela": rela,
                "sab": sab,
            }
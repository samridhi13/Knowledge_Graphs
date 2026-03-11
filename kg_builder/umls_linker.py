from typing import Dict, List

import spacy

try:
    import scispacy  # noqa: F401
    from scispacy.abbreviation import AbbreviationDetector
    from scispacy.linking import EntityLinker  # registers scispacy_linker
except ImportError as e:
    raise ImportError(
        "scispaCy is not installed correctly. "
        "Run: pip install scispacy==0.5.4 "
        "and install the matching model tar.gz."
    ) from e


class UMLSLinkerWrapper:
    def __init__(self, model_name: str = "en_core_sci_sm", threshold: float = 0.85):
        self.threshold = threshold

        try:
            self.nlp = spacy.load(model_name)
        except Exception as e:
            raise RuntimeError(
                f"Could not load spaCy model '{model_name}'. "
                f"Make sure the matching scispaCy model is installed."
            ) from e

        if "abbreviation_detector" not in self.nlp.pipe_names:
            self.nlp.add_pipe("abbreviation_detector")

        if "scispacy_linker" not in self.nlp.pipe_names:
            self.nlp.add_pipe(
                "scispacy_linker",
                config={
                    "resolve_abbreviations": True,
                    "linker_name": "umls",
                },
            )

        self.linker = self.nlp.get_pipe("scispacy_linker")

    def link_text(self, text: str, section: str) -> List[Dict]:
        if not text or not text.strip():
            return []

        doc = self.nlp(text)
        out = []

        for ent in doc.ents:
            kb_ents = getattr(ent._, "kb_ents", [])
            for cui, score in kb_ents:
                if score < self.threshold:
                    continue

                kb_ent = self.linker.kb.cui_to_entity.get(cui)
                canonical_name = getattr(kb_ent, "canonical_name", None) if kb_ent else None
                types = list(getattr(kb_ent, "types", []) or []) if kb_ent else []

                out.append(
                    {
                        "mention": ent.text,
                        "section": section,
                        "cui": cui,
                        "score": float(score),
                        "canonical_name": canonical_name,
                        "types": types,
                    }
                )

        return out
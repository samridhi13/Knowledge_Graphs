# Knowledge_Graphs
# Genetic Disease Literature Knowledge Graph

This project builds a **biomedical literature knowledge graph** from PubMed papers related to genetic diseases.

The goal is to transform unstructured biomedical literature into a **structured graph representation** connecting:

- scientific papers
- authors
- biomedical concepts (UMLS CUIs)

This graph can later be used for:

- biomedical paper recommendation
- knowledge discovery
- literature exploration
- retrieval-augmented generation (RAG)
- fairness-aware recommendation systems

---

# Project Goal

Biomedical literature is growing extremely fast. Researchers often struggle to find relevant papers among thousands of publications.

This project creates a **knowledge graph from PubMed papers** and connects them to biomedical ontologies such as **UMLS (Unified Medical Language System)**.

The knowledge graph will allow us to:

- discover relationships between research papers
- explore biomedical concepts across papers
- build intelligent recommendation systems
- support LLM-based literature analysis

---

# Current Stage (Phase 1)

At the current stage the system builds a **base literature knowledge graph**.

Pipeline steps:

1. Query PubMed using the NCBI API
2. Download paper metadata
3. Extract biomedical entities from text
4. Map entities to **UMLS Concept Unique Identifiers (CUIs)**
5. Build a graph linking papers, authors, and biomedical concepts

---

# Knowledge Graph Structure

## Nodes

The graph currently contains three types of nodes.

### Paper

Represents a scientific publication.

Example:

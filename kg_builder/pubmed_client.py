import re
import time
from typing import Dict, List, Optional

import requests
from lxml import etree

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


def _extract_year(article_elem) -> Optional[int]:
    # Try ArticleDate first
    year = article_elem.xpath(".//ArticleDate/Year/text()")
    if year:
        try:
            return int(year[0])
        except Exception:
            pass

    # Then try PubDate/Year
    year = article_elem.xpath(".//JournalIssue/PubDate/Year/text()")
    if year:
        try:
            return int(year[0])
        except Exception:
            pass

    # Then MedlineDate like "2018 Jan-Feb"
    medline_date = article_elem.xpath(".//JournalIssue/PubDate/MedlineDate/text()")
    if medline_date:
        m = re.search(r"(19|20)\d{2}", medline_date[0])
        if m:
            return int(m.group(0))

    return None


def _join_text(elem) -> str:
    if elem is None:
        return ""
    return " ".join(" ".join(elem.itertext()).split())


def search_pmids(query: str, retmax: int, email: str, api_key: Optional[str] = None) -> List[str]:
    params = {
        "db": "pubmed",
        "term": query,
        "retmax": retmax,
        "retmode": "json",
        "sort": "relevance",
        "email": email,
    }
    if api_key:
        params["api_key"] = api_key

    resp = requests.get(f"{EUTILS_BASE}/esearch.fcgi", params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("esearchresult", {}).get("idlist", [])


def fetch_pubmed_records(pmids: List[str], email: str, api_key: Optional[str] = None, batch_size: int = 100) -> List[Dict]:
    records = []
    for i in range(0, len(pmids), batch_size):
        batch = pmids[i:i + batch_size]
        params = {
            "db": "pubmed",
            "id": ",".join(batch),
            "retmode": "xml",
            "email": email,
        }
        if api_key:
            params["api_key"] = api_key

        resp = requests.get(f"{EUTILS_BASE}/efetch.fcgi", params=params, timeout=120)
        resp.raise_for_status()

        root = etree.fromstring(resp.content)

        for article in root.xpath(".//PubmedArticle"):
            pmid_text = article.xpath("./MedlineCitation/PMID/text()")
            pmid = pmid_text[0].strip() if pmid_text else None
            if not pmid:
                continue

            title_elems = article.xpath(".//Article/ArticleTitle")
            title = _join_text(title_elems[0]) if title_elems else ""

            abstract_elems = article.xpath(".//Article/Abstract/AbstractText")
            abstract_parts = [_join_text(x) for x in abstract_elems]
            abstract = "\n".join([x for x in abstract_parts if x])

            year = _extract_year(article)

            authors = []
            author_elems = article.xpath(".//Article/AuthorList/Author")
            for a in author_elems:
                collective = a.xpath("./CollectiveName/text()")
                if collective:
                    authors.append(collective[0].strip())
                    continue

                fore = a.xpath("./ForeName/text()")
                last = a.xpath("./LastName/text()")
                name = " ".join([x for x in [fore[0].strip() if fore else "", last[0].strip() if last else ""] if x])
                if name:
                    authors.append(name)

            journal = ""
            journal_elems = article.xpath(".//Article/Journal/Title/text()")
            if journal_elems:
                journal = journal_elems[0].strip()

            records.append(
                {
                    "pmid": pmid,
                    "title": title,
                    "abstract": abstract,
                    "year": year,
                    "authors": authors,
                    "journal": journal,
                }
            )

        # polite rate limiting
        time.sleep(0.35 if api_key else 0.8)

    return records
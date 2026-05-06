"""
Batch-analyse sustainability PDFs: CSV metadata + raw page text (rawoutput.txt).

Run from repo root with the virtual environment activated, for example:
  python analyse_pdfs.py {folder to analyze}
"""

import os
import sys

import pandas as pd
import pdfplumber

KEYWORDS = {
    "keyword_TCFD": ["tcfd"],
    "keyword_GRI": ["gri"],
    "keyword_SASB": ["sasb"],
    "keyword_IFRS": ["ifrs"],
    "keyword_scope1": ["scope 1", "scope one", "scope-1", "scope-one"],
    "keyword_scope2": ["scope 2", "scope two", "scope-2", "scope-two"],
    "keyword_scope3": ["scope 3", "scope three", "scope-3", "scope-three"],
    "keyword_netzero": ["net zero", "net-zero"],
    "keyword_carbonprice": ["carbon price", "carbon pricing", "carbon-price", "carbon-pricing"],
}


def _guess_company_name(filename: str) -> str:
    stem = os.path.splitext(os.path.basename(filename))[0].strip()
    if not stem:
        return ""
    for sep in [" — ", " – ", " - ", "_"]:
        if sep in stem:
            return stem.split(sep)[0].strip()
    return stem.replace("_", " ").strip()


def _format_pages(pages: set[int]) -> str:
    if not pages:
        return "0"
    return ",".join(str(p) for p in sorted(pages))


"""
PDF structure / keyword scan per project metadata:
file_name, company_name, total_pages, file_size_mb, keyword columns,
has_table, table_page_numbers, text_density_profile, extractable, notes.
"""
def analyse_pdf(filepath: str) -> dict:
    metadata = {
        "file_name": os.path.basename(filepath),
        "company_name": _guess_company_name(filepath),
        "total_pages": 0,
        "file_size_mb": round(os.path.getsize(filepath) / (1024 * 1024), 2),
        "keyword_TCFD": "0",
        "keyword_GRI": "0",
        "keyword_SASB": "0",
        "keyword_IFRS": "0",
        "keyword_scope1": "0",
        "keyword_scope2": "0",
        "keyword_scope3": "0",
        "keyword_netzero": "0",
        "keyword_carbonprice": "0",
        "has_table": "No",
        "table_page_numbers": "",
        "text_density_profile": "",
        "extractable": "No",
        "notes": "",
    }

    keyword_pages = {k: set() for k in KEYWORDS}
    table_pages: set[int] = set()
    page_char_counts: list[tuple[int, int]] = []

    try:
        with pdfplumber.open(filepath) as pdf:
            metadata["total_pages"] = len(pdf.pages)
            total_chars = 0

            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                total_chars += len(text)
                page_char_counts.append((page_num, len(text)))
                lower = text.lower()

                # regex pattern matching implementation may be superior? 
                for col_name, phrases in KEYWORDS.items():
                    for phrase in phrases:
                        if phrase in lower:
                            keyword_pages[col_name].add(page_num)
                            break

                try:
                    if page.find_tables():
                        table_pages.add(page_num)
                except Exception:
                    tbls = page.extract_tables()
                    if tbls and any(t for t in tbls if t):
                        table_pages.add(page_num)

            for col_name in KEYWORDS:
                metadata[col_name] = _format_pages(keyword_pages[col_name])

            if table_pages:
                metadata["has_table"] = "Yes"
                metadata["table_page_numbers"] = _format_pages(table_pages)

            page_char_counts.sort(key=lambda x: x[1], reverse=True)
            metadata["text_density_profile"] = ",".join(
                str(p) for p, _ in page_char_counts[:5]
            )

            metadata["extractable"] = "Yes" if total_chars >= 200 else "No"

    except Exception as exc:
        metadata["notes"] = str(exc)[:2000]

    return metadata


CSV_COLUMNS = [
    "file_name",
    "company_name",
    "total_pages",
    "file_size_mb",
    "keyword_TCFD",
    "keyword_GRI",
    "keyword_SASB",
    "keyword_IFRS",
    "keyword_scope1",
    "keyword_scope2",
    "keyword_scope3",
    "keyword_netzero",
    "keyword_carbonprice",
    "has_table",
    "table_page_numbers",
    "text_density_profile",
    "extractable",
    "notes",
]

"""
Writes the raw parsed text to a file
ARGUMENTS:
- pdf_paths: list[str] - List of PDF file paths to parse
- raw_output_path: str - Path to the output file
RETURNS:
- None
"""
def write_raw_output(pdf_paths: list[str], raw_output_path: str) -> None:
    parent = os.path.dirname(raw_output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(raw_output_path, "w", encoding="utf-8") as out:
        for idx, pdf_path in enumerate(pdf_paths, start=1):
            out.write(f"=== FILE: {os.path.basename(pdf_path)} ===\n")
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, start=1):
                    text = page.extract_text() or ""
                    out.write(f"\n--- PAGE {page_num} ---\n")
                    if text:
                        out.write(text.rstrip() + "\n")
            if idx < len(pdf_paths):
                out.write("\n\n")


"""
Parses the PDFs and writes the metadata as a row in the csv file
Key Varriables: ( could be passed as arguments instead of hardcoded )
- pdf_folder: str - Path to the folder containing the PDFs
- out_path: str - Path to the output csv file
- raw_out_path: str - Path to the output raw text file
RETURNS:
- None
"""
if __name__ == "__main__":
    _default_pdf_folder = os.path.join(os.path.dirname(__file__), "..", "SusReports")
    pdf_folder = os.path.normpath(sys.argv[1] if len(sys.argv) > 1 else _default_pdf_folder)
    results = []

    pdf_paths_for_raw: list[str] = []
    if os.path.isdir(pdf_folder):
        for curReportNumber,filename in enumerate(sorted(os.listdir(pdf_folder))):
            print(f"Processing report {curReportNumber + 1}")
            if filename.lower().endswith(".pdf"):
                fp = os.path.join(pdf_folder, filename)
                results.append(analyse_pdf(fp))
                pdf_paths_for_raw.append(fp)
    df = pd.DataFrame(results, columns=CSV_COLUMNS if results else None)
    out_path = os.path.join(
        os.path.dirname(__file__), "..", "sus_reports_analysis_TEST.csv"
    )
    out_path = os.path.normpath(out_path)
    df.to_csv(out_path, index=False)
    raw_out_path = os.path.join(os.path.dirname(__file__), "..", "rawoutput.txt")
    raw_out_path = os.path.normpath(raw_out_path)

    # Test raw output

    # if pdf_paths_for_raw:
    #     write_raw_output(pdf_paths_for_raw, raw_out_path)
    print(f"Done. {len(results)} files processed -> {out_path}")

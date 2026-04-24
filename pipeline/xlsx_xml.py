from __future__ import annotations

import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterator


NS_MAIN = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
NS_PKG_REL = {"pr": "http://schemas.openxmlformats.org/package/2006/relationships"}


_CELL_REF_RE = re.compile(r"^([A-Z]+)")


def _column_index(cell_ref: str) -> int:
    m = _CELL_REF_RE.match(cell_ref)
    if not m:
        return -1
    letters = m.group(1)
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def _load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    shared: list[str] = []
    if "xl/sharedStrings.xml" not in zf.namelist():
        return shared

    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    for si in root.findall("m:si", NS_MAIN):
        plain_t = si.find("m:t", NS_MAIN)
        if plain_t is not None and plain_t.text is not None:
            shared.append(plain_t.text)
            continue

        chunks: list[str] = []
        for run in si.findall("m:r", NS_MAIN):
            t = run.find("m:t", NS_MAIN)
            if t is not None and t.text is not None:
                chunks.append(t.text)
        shared.append("".join(chunks))
    return shared


def _resolve_first_sheet_xml_path(zf: zipfile.ZipFile) -> str:
    wb = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

    rid_to_target: dict[str, str] = {}
    for rel in rels.findall("pr:Relationship", NS_PKG_REL):
        rid = rel.attrib.get("Id", "")
        target = rel.attrib.get("Target", "")
        if rid and target:
            rid_to_target[rid] = target

    sheets = wb.find("m:sheets", NS_MAIN)
    if sheets is None:
        raise ValueError("Workbook has no sheets")

    first_sheet = sheets.find("m:sheet", NS_MAIN)
    if first_sheet is None:
        raise ValueError("Workbook has no first sheet")

    rid = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
    if not rid:
        raise ValueError("Sheet relationship id not found")

    target = rid_to_target.get(rid)
    if not target:
        raise ValueError("Sheet relationship target not found")

    candidate = target.replace("\\", "/")
    if not candidate.startswith("xl/"):
        candidate = f"xl/{candidate}"

    if candidate in zf.namelist():
        return candidate

    candidate = target.lstrip("/")
    if candidate in zf.namelist():
        return candidate

    raise ValueError(f"Cannot resolve sheet XML path for target '{target}'")


def read_first_sheet_rows(path: Path) -> Iterator[list[str]]:
    with zipfile.ZipFile(path) as zf:
        shared = _load_shared_strings(zf)
        sheet_xml_path = _resolve_first_sheet_xml_path(zf)
        root = ET.fromstring(zf.read(sheet_xml_path))

        sheet_data = root.find("m:sheetData", NS_MAIN)
        if sheet_data is None:
            return

        for row in sheet_data.findall("m:row", NS_MAIN):
            cells: dict[int, str] = {}
            max_col = -1

            for c in row.findall("m:c", NS_MAIN):
                cell_ref = c.attrib.get("r", "")
                idx = _column_index(cell_ref)
                if idx < 0:
                    idx = max_col + 1

                cell_type = c.attrib.get("t", "")
                value = ""

                v = c.find("m:v", NS_MAIN)
                if v is not None and v.text is not None:
                    raw = v.text
                    if cell_type == "s":
                        try:
                            value = shared[int(raw)]
                        except (ValueError, IndexError):
                            value = raw
                    elif cell_type == "b":
                        value = "TRUE" if raw == "1" else "FALSE"
                    else:
                        value = raw
                else:
                    is_elem = c.find("m:is", NS_MAIN)
                    if is_elem is not None:
                        t = is_elem.find("m:t", NS_MAIN)
                        if t is not None and t.text is not None:
                            value = t.text

                cells[idx] = value
                if idx > max_col:
                    max_col = idx

            if max_col < 0:
                yield []
                continue

            out = ["" for _ in range(max_col + 1)]
            for idx, value in cells.items():
                out[idx] = value
            yield out


def max_columns(path: Path) -> int:
    width = 0
    for row in read_first_sheet_rows(path):
        if len(row) > width:
            width = len(row)
    return width

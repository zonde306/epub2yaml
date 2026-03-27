from __future__ import annotations

import hashlib
import os.path
import zipfile
from pathlib import PurePosixPath

import lxml.etree
import lxml.html

from models import Chapter

HTML_EXTENSION = {".html", ".xhtml", ".htm"}
STRIP_TEXT = " \r\n\t\u3000　"
TOKEN_ESTIMATE_DIVISOR = 4
CONTAINER_XML_PATH = "META-INF/container.xml"


def extract_epub(epub_path: str) -> list[Chapter]:
    """
    Extract ordered chapter content from an EPUB file.

    The implementation first attempts to follow the OPF spine order.
    If the spine cannot be resolved, it falls back to the ZIP entry order.
    """

    file_data: dict[str, bytes] = {}

    with zipfile.ZipFile(epub_path, "r") as archive:
        for file_name in archive.namelist():
            file_data[file_name] = archive.read(file_name)

    ordered_html_files = resolve_spine_html_files(file_data)
    if not ordered_html_files:
        ordered_html_files = [
            file_name
            for file_name in file_data.keys()
            if os.path.splitext(file_name)[1].lower() in HTML_EXTENSION
        ]

    results: list[Chapter] = []

    for file_name in ordered_html_files:
        file_content = file_data[file_name]
        title, content = extract_html(file_content)
        normalized_content = normalize_text(content)
        if not normalized_content:
            continue

        chapter_index = len(results)
        normalized_title = normalize_title(title, chapter_index)
        chapter_id = build_chapter_id(file_name, normalized_title, normalized_content, chapter_index)
        results.append(
            Chapter(
                chapter_index=chapter_index + 1,
                chapter_id=chapter_id,
                title=normalized_title,
                text=normalized_content,
                source_path=file_name,
                token_estimate=estimate_tokens(normalized_content),
            )
        )

    return results


def resolve_spine_html_files(file_data: dict[str, bytes]) -> list[str]:
    container_bytes = file_data.get(CONTAINER_XML_PATH)
    if not container_bytes:
        return []

    try:
        container_root = lxml.etree.fromstring(container_bytes)
        rootfiles = container_root.xpath("//*[local-name()='rootfile']/@full-path")
        if not rootfiles:
            return []

        opf_path = str(rootfiles[0])
        opf_bytes = file_data.get(opf_path)
        if not opf_bytes:
            return []

        opf_root = lxml.etree.fromstring(opf_bytes)
        manifest: dict[str, str] = {}
        for item in opf_root.xpath("//*[local-name()='manifest']/*[local-name()='item']"):
            item_id = item.attrib.get("id")
            href = item.attrib.get("href")
            if not item_id or not href:
                continue
            resolved_path = resolve_relative_path(opf_path, href)
            manifest[item_id] = resolved_path

        ordered_files: list[str] = []
        for itemref in opf_root.xpath("//*[local-name()='spine']/*[local-name()='itemref']"):
            idref = itemref.attrib.get("idref")
            if not idref:
                continue
            resolved = manifest.get(idref)
            if not resolved:
                continue
            if os.path.splitext(resolved)[1].lower() not in HTML_EXTENSION:
                continue
            if resolved in file_data:
                ordered_files.append(resolved)

        return ordered_files
    except (lxml.etree.XMLSyntaxError, ValueError, TypeError):
        return []


def resolve_relative_path(base_file: str, relative_path: str) -> str:
    base_dir = PurePosixPath(base_file).parent
    resolved = base_dir.joinpath(relative_path)
    return str(resolved)


def extract_html(html_content: bytes) -> tuple[str, str]:
    """
    Extract text content from HTML bytes.

    Args:
        html_content: HTML bytes.

    Returns:
        Tuple of (title, content).
    """

    doc = lxml.html.fromstring(html_content)

    for p in doc.xpath(r"//p[@style='opacity:0.4;']"):
        if isinstance(p, lxml.etree._Element) and p.getparent() is not None:
            p.getparent().remove(p)

    for dom in doc.xpath(r"//*[contains(@style,'writing-mode:vertical-rl;')]"):
        if isinstance(dom, lxml.etree._Element):
            dom.attrib["style"] = ""

    title = "Unnamed Chapter"
    if h1 := doc.xpath("//h1"):
        first_title = h1[0].text_content().strip(STRIP_TEXT)
        title = first_title or "Unnamed Chapter"

    contents: list[str] = []

    for p in doc.xpath(r"//p"):
        if not isinstance(p, lxml.etree._Element):
            continue

        if p.attrib.get("style") == "opacity:0.4;":
            continue

        text = p.text_content().strip(STRIP_TEXT)
        if text:
            contents.append(text)

    return title, "\n".join(contents)


def normalize_text(value: str) -> str:
    lines = [line.strip(STRIP_TEXT) for line in value.splitlines()]
    return "\n".join(line for line in lines if line)


def normalize_title(value: str, chapter_index: int) -> str:
    normalized = value.strip(STRIP_TEXT)
    if normalized:
        return normalized
    return f"Chapter {chapter_index + 1}"


def build_chapter_id(source_path: str, title: str, content: str, chapter_index: int) -> str:
    digest = hashlib.sha256(f"{source_path}\n{title}\n{content}".encode("utf-8")).hexdigest()[:12]
    return f"ch{chapter_index + 1:04d}-{digest}"


def estimate_tokens(value: str) -> int:
    return max(1, (len(value) + TOKEN_ESTIMATE_DIVISOR - 1) // TOKEN_ESTIMATE_DIVISOR)

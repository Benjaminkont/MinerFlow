import argparse
import json
import re
from html import unescape
from pathlib import Path
from typing import Any

# MOJIBAKE_HINTS 乱码判断，之后可优化
MOJIBAKE_HINTS = "æÆçÇèéêëîïðñòóôõöùúûüýÿ鈥滃€傛灄涓夐噾锛�"
CHUNK_NAME_RE = re.compile(r"chunk_(\d+)\.json$", re.IGNORECASE)


def load_chunk(filepath: Path) -> list[dict[str, Any]]:
    if not filepath.exists():
        raise FileNotFoundError(f"找不到文件: {filepath}")

    with filepath.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"输入 JSON 不是 block 数组: {filepath}")

    blocks: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            blocks.append(item)
    return blocks


def mojibake_score(text: str) -> int:
    return sum(text.count(ch) for ch in MOJIBAKE_HINTS) + text.count("�") * 3


def try_redecode(text: str, source_encoding: str, target_encoding: str) -> str:
    try:
        return text.encode(source_encoding).decode(target_encoding)
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def repair_text(text: str) -> str:
    if not text:
        return text

    candidates = [text]
    for source_encoding, target_encoding in [
        ("gbk", "utf-8"),
        ("gb18030", "utf-8"),
        ("latin1", "utf-8"),
    ]:
        candidate = try_redecode(text, source_encoding, target_encoding)
        candidates.append(candidate)

    best = min(candidates, key=mojibake_score)
    return best


def normalize_whitespace(text: str) -> str:
    text = unescape(text)
    text = repair_text(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def block_to_markdown(block: dict[str, Any]) -> list[str]:
    block_type = str(block.get("type", "")).strip().lower()

    if block_type == "page":
        page = block.get("page")
        return ["---", f"*Page {page}*", ""]

    if block_type == "title":
        level = block.get("level", 1)
        try:
            level = int(level)
        except (TypeError, ValueError):
            level = 1
        level = min(max(level, 1), 6)
        text = normalize_whitespace(str(block.get("text", "")))
        return [f"{'#' * level} {text}", ""] if text else []

    if block_type == "paragraph":
        text = normalize_whitespace(str(block.get("text", "")))
        return [text, ""] if text else []

    if block_type == "table":
        lines: list[str] = []
        caption = normalize_whitespace(str(block.get("caption", "")))
        html = str(block.get("html", "")).strip()
        footnote = normalize_whitespace(str(block.get("footnote", "")))

        if caption:
            lines.append(f"**{caption}**")
            lines.append("")
        if html:
            lines.append(unescape(html))
            lines.append("")
        if footnote:
            lines.append(footnote)
            lines.append("")
        return lines

    return []


def chunk_to_markdown(blocks: list[dict[str, Any]]) -> str:
    md_lines: list[str] = []
    for block in blocks:
        md_lines.extend(block_to_markdown(block))

    while md_lines and md_lines[-1] == "":
        md_lines.pop()

    return "\n".join(md_lines) + "\n"


def convert_file(input_filepath: Path, output_filepath: Path) -> None:
    blocks = load_chunk(input_filepath)
    markdown = chunk_to_markdown(blocks)
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    with output_filepath.open("w", encoding="utf-8") as f:
        f.write(markdown)


def convert_directory(input_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    chunk_files = sorted(input_dir.glob("chunk_*.json"))
    if not chunk_files:
        raise FileNotFoundError(f"目录中没有找到 chunk_*.json: {input_dir}")

    for chunk_file in chunk_files:
        output_file = output_dir / f"{chunk_file.stem}.md"
        convert_file(chunk_file, output_file)


def parse_chunk_index(chunk_file: Path) -> int:
    match = CHUNK_NAME_RE.match(chunk_file.name)
    if not match:
        raise ValueError(f"非法 chunk 文件名: {chunk_file.name}")
    return int(match.group(1))


def find_doc_dirs(root_dir: Path) -> list[Path]:
    return sorted(
        [
            path
            for path in root_dir.iterdir()
            if path.is_dir() and any(path.glob("chunk_*.json"))
        ]
    )


def list_chunk_files(doc_dir: Path) -> list[Path]:
    chunk_files = [path for path in doc_dir.glob("chunk_*.json") if path.is_file()]
    return sorted(chunk_files, key=parse_chunk_index)


def merge_chunks_to_markdown(doc_dir: Path, separator: str) -> str:
    merged_parts: list[str] = []
    previous_index: int | None = None

    for chunk_file in list_chunk_files(doc_dir):
        current_index = parse_chunk_index(chunk_file)
        blocks = load_chunk(chunk_file)
        markdown = chunk_to_markdown(blocks).rstrip()
        if not markdown:
            previous_index = current_index
            continue

        if merged_parts:
            if previous_index is not None and current_index == previous_index + 1:
                merged_parts.append("")
            else:
                merged_parts.append(separator.rstrip())
                merged_parts.append("")

        merged_parts.append(markdown)
        previous_index = current_index

    if not merged_parts:
        return ""
    return "\n".join(merged_parts).rstrip() + "\n"


def rebuild_all(new_chunk_dir: Path, output_dir: Path, separator: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    doc_dirs = find_doc_dirs(new_chunk_dir)
    if not doc_dirs:
        raise FileNotFoundError(f"未找到任何 chunk 文件夹: {new_chunk_dir}")

    for doc_dir in doc_dirs:
        markdown = merge_chunks_to_markdown(doc_dir, separator=separator)
        output_file = output_dir / f"{doc_dir.name}.md"
        with output_file.open("w", encoding="utf-8") as f:
            f.write(markdown)
        print(f"已生成: {output_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert chunk JSON files to Markdown.")
    parser.add_argument(
        "--mode",
        choices=["convert", "rebuild"],
        default="convert",
        help="convert: 单个 chunk 或 chunk 文件夹转 markdown；rebuild: 遍历 new_chunk 下文档文件夹并重组为 extracted_data",
    )
    parser.add_argument(
        "input_path",
        nargs="?",
        default="chunk_json",
        help="输入路径。convert 模式下可为 chunk 文件或 chunk 文件夹；rebuild 模式下应为 new_chunk 大目录",
    )
    parser.add_argument(
        "--output",
        default="chunk_markdown",
        help="输出路径。convert 模式下可为 markdown 文件或目录；rebuild 模式下应为 extracted_data 目录",
    )
    parser.add_argument(
        "--separator",
        default="\n---\n",
        help="rebuild 模式下，当 chunk 编号不连续时插入的分隔线内容",
    )
    args = parser.parse_args()

    input_path = Path(args.input_path).resolve()
    output_path = Path(args.output).resolve()

    if args.mode == "rebuild":
        rebuild_all(input_path, output_path, separator=args.separator)
    else:
        if input_path.is_dir():
            convert_directory(input_path, output_path)
        else:
            if output_path.suffix.lower() != ".md":
                output_path = output_path / f"{input_path.stem}.md"
            convert_file(input_path, output_path)


if __name__ == "__main__":
    main()

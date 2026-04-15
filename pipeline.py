import argparse
import json
import shutil
from pathlib import Path

from chunk_blocks import chunk_blocks, load_blocks
from iterate_chunks import iter_chunk_tasks, write_manifest
from mv_rm import process_root
from Reorgnize import rebuild_all
from screen_reorganize import convert_waiting_chunk


SUFFIX = "_content_list_v2"


def doc_folder_name(json_file: Path) -> str:
    stem = json_file.stem
    if "__" in stem:
        stem = stem.split("__", 1)[1]
    if stem.endswith(SUFFIX):
        return stem[: -len(SUFFIX)]
    return stem


def chunk_one_json(json_file: Path, waiting_chunk_root: Path, char_limit: int) -> Path:
    output_dir = waiting_chunk_root / doc_folder_name(json_file)
    output_dir.mkdir(parents=True, exist_ok=True)
    blocks = load_blocks(json_file)
    chunk_blocks(blocks=blocks, output_dir=output_dir, char_limit=char_limit)
    return output_dir


def chunk_all_jsons(json_root: Path, waiting_chunk_root: Path, char_limit: int) -> list[Path]:
    json_files = sorted(json_root.glob("*_content_list_v2.json"))
    if not json_files:
        raise FileNotFoundError(f"未找到任何 *_content_list_v2.json: {json_root}")

    waiting_chunk_root.mkdir(parents=True, exist_ok=True)
    created_dirs: list[Path] = []
    for json_file in json_files:
        chunk_dir = chunk_one_json(json_file, waiting_chunk_root, char_limit)
        created_dirs.append(chunk_dir)
        print(f"已分块 {json_file} -> {chunk_dir}")
    return created_dirs


def build_manifest(waiting_chunk_root: Path, manifest_file: Path) -> None:
    tasks = iter_chunk_tasks(waiting_chunk_root)
    if not tasks:
        raise FileNotFoundError(f"未找到任何 chunk 任务: {waiting_chunk_root}")
    write_manifest(tasks, manifest_file)
    print(f"已生成任务清单 {manifest_file}")
    print(f"任务数量: {len(tasks)}")


def build_screen_prompt(markdown_text: str, rule_text: str) -> str:
    return (
        "你是一个严格的文本初筛模型。\n"
        "你的任务是根据给定规则判断下面的文档片段是否应当保留进入下一轮。\n"
        "你只能输出 YES 或 NO，不要输出任何解释、标点、空格或其他内容。\n\n"
        f"筛选规则:\n{rule_text}\n\n"
        "待判断内容:\n"
        f"{markdown_text}"
    )


def build_extract_prompt(markdown_text: str, doc_id: str, schema_text: str) -> str:
    return (
        "你是一个财报结构化抽取助手。\n"
        "你的任务是从下面的财报 markdown 中抽取结构化原子数据，并且只输出 JSON。\n"
        "不要输出解释，不要输出 markdown 代码块，不要输出额外文字。\n"
        "如果没有把握，不要编造；没有抽到的数据可以省略 facts 项中的对应记录。\n\n"
        "输出要求：\n"
        "1. 顶层必须是一个 JSON 对象。\n"
        "2. 顶层至少包含 doc_id、stock_code、stock_abbr、report_year、report_period、facts。\n"
        "3. facts 必须是数组，每个元素尽量包含 statement、scope、metric_std、metric_alias、value、value_raw、unit、time_role、table_title、source_chunk、source_text。\n"
        "4. 如果无法确定 stock_code、stock_abbr、report_year、report_period，可以填 null。\n"
        "5. 只输出一个 JSON 对象。\n\n"
        f"抽取 schema 参考：\n{schema_text}\n\n"
        f"文档标识：{doc_id}\n\n"
        "财报 markdown：\n"
        f"{markdown_text}"
    )


def normalize_screen_result(answer: str) -> str:
    text = answer.strip().upper()
    if text == "YES":
        return "YES"
    if text == "NO":
        return "NO"
    if "YES" in text and "NO" not in text:
        return "YES"
    if "NO" in text and "YES" not in text:
        return "NO"
    return "INVALID"


def parse_json_response(answer: str) -> dict | None:
    text = answer.strip()
    if not text:
        return None

    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None

    try:
        data = json.loads(text[start : end + 1])
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None


def copy_yes_chunk(chunk_json_file: Path, new_chunk_root: Path, doc_id: str) -> Path:
    target_dir = new_chunk_root / doc_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / chunk_json_file.name
    shutil.copy2(chunk_json_file, target_file)
    return target_file


def write_json(data: object, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def run_screen(
    waiting_chunk_root: Path,
    waiting_markdown_root: Path,
    new_chunk_root: Path,
    results_file: Path,
    model_path: str,
    rule_text: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    gpu_memory_utilization: float,
) -> None:
    from vllm_service import LLM

    convert_waiting_chunk(waiting_chunk_root, waiting_markdown_root)

    llm = LLM(
        _modelpath=model_path,
        _temperature=temperature,
        _top_p=top_p,
        _max_tokens=max_tokens,
        _gpu_memory_utilization=gpu_memory_utilization,
    )
    llm.load_model()

    results: list[dict[str, str]] = []
    try:
        for doc_dir in sorted(path for path in waiting_markdown_root.iterdir() if path.is_dir()):
            doc_id = doc_dir.name
            for chunk_md_file in sorted(doc_dir.glob("chunk_*.md")):
                markdown_text = chunk_md_file.read_text(encoding="utf-8")
                prompt = build_screen_prompt(markdown_text, rule_text)
                raw_answer = llm.chat(prompt)
                screen_result = normalize_screen_result(raw_answer)

                chunk_json_file = waiting_chunk_root / doc_id / f"{chunk_md_file.stem}.json"
                copied_to = ""
                if screen_result == "YES":
                    copied_to = str(copy_yes_chunk(chunk_json_file, new_chunk_root, doc_id).resolve())

                results.append(
                    {
                        "doc_id": doc_id,
                        "chunk_id": chunk_md_file.stem,
                        "chunk_markdown_path": str(chunk_md_file.resolve()),
                        "chunk_json_path": str(chunk_json_file.resolve()),
                        "raw_answer": raw_answer.strip(),
                        "screen_result": screen_result,
                        "copied_to": copied_to,
                    }
                )
                print(f"[{screen_result}] {doc_id} / {chunk_md_file.stem}")
    finally:
        llm.unload_model()

    write_json(results, results_file)
    print(f"已生成初筛结果 {results_file}")


def run_prepare(
    root_o: Path,
    extracted_json_root: Path,
    waiting_chunk_root: Path,
    manifest_file: Path,
    char_limit: int,
    dry_run: bool,
) -> None:
    process_root(root_o=root_o, root_x=extracted_json_root, dry_run=dry_run)
    if dry_run:
        print("当前为 dry-run，未执行后续分块和清单生成。")
        return
    chunk_all_jsons(extracted_json_root, waiting_chunk_root, char_limit)
    build_manifest(waiting_chunk_root, manifest_file)


def run_rebuild(new_chunk_root: Path, extracted_data_root: Path, separator: str) -> None:
    rebuild_all(new_chunk_root, extracted_data_root, separator=separator)


def run_extract(
    extracted_data_root: Path,
    extracted_json_root: Path,
    results_file: Path,
    model_path: str,
    schema_text: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    gpu_memory_utilization: float,
) -> None:
    from vllm_service import LLM

    markdown_files = sorted(extracted_data_root.glob("*.md"))
    if not markdown_files:
        raise FileNotFoundError(f"未找到任何 markdown 文件: {extracted_data_root}")

    extracted_json_root.mkdir(parents=True, exist_ok=True)

    llm = LLM(
        _modelpath=model_path,
        _temperature=temperature,
        _top_p=top_p,
        _max_tokens=max_tokens,
        _gpu_memory_utilization=gpu_memory_utilization,
    )
    llm.load_model()

    results: list[dict[str, str]] = []
    try:
        for markdown_file in markdown_files:
            doc_id = markdown_file.stem
            markdown_text = markdown_file.read_text(encoding="utf-8")
            prompt = build_extract_prompt(markdown_text, doc_id, schema_text)
            raw_answer = llm.chat(prompt)
            parsed = parse_json_response(raw_answer)

            output_file = extracted_json_root / f"{doc_id}.json"
            status = "SUCCESS"
            error = ""

            if parsed is None:
                status = "PARSE_ERROR"
                error = "模型返回内容无法解析为 JSON 对象"
            else:
                parsed.setdefault("doc_id", doc_id)
                parsed.setdefault("stock_code", None)
                parsed.setdefault("stock_abbr", None)
                parsed.setdefault("report_year", None)
                parsed.setdefault("report_period", None)
                if not isinstance(parsed.get("facts"), list):
                    parsed["facts"] = []
                write_json(parsed, output_file)

            results.append(
                {
                    "doc_id": doc_id,
                    "markdown_path": str(markdown_file.resolve()),
                    "output_file": str(output_file.resolve()) if status == "SUCCESS" else "",
                    "status": status,
                    "error": error,
                    "raw_answer": raw_answer.strip(),
                }
            )
            print(f"[{status}] {doc_id}")
    finally:
        llm.unload_model()

    write_json(results, results_file)
    print(f"已生成细筛结果 {results_file}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline controller for prepare, chunking, screening, rebuild, and extraction."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="从 O 中提取 JSON 到集中目录，并分块到 waiting_chunk，再生成 manifest")
    prepare_parser.add_argument("root_o", help="总目录 O，下面包含很多 A 目录")
    prepare_parser.add_argument("--json-root", default="collected_json", help="集中存放提取后 JSON 的目录")
    prepare_parser.add_argument("--waiting-chunk", default="waiting_chunk", help="存放 chunk 文件夹的大目录")
    prepare_parser.add_argument("--manifest", default="chunk_task_manifest.json", help="待筛选任务清单输出路径")
    prepare_parser.add_argument("--char-limit", type=int, default=6000, help="分块字数阈值")
    prepare_parser.add_argument("--execute", action="store_true", help="实际执行提取和删除。默认仅 dry-run")

    chunk_parser = subparsers.add_parser("chunk", help="对已集中存放的 JSON 批量分块")
    chunk_parser.add_argument("json_root", help="存放 *_content_list_v2.json 的目录")
    chunk_parser.add_argument("--waiting-chunk", default="waiting_chunk", help="存放 chunk 文件夹的大目录")
    chunk_parser.add_argument("--char-limit", type=int, default=6000, help="分块字数阈值")

    manifest_parser = subparsers.add_parser("manifest", help="遍历 waiting_chunk，生成待筛选任务清单")
    manifest_parser.add_argument("waiting_chunk_root", help="存放 chunk 文件夹的大目录")
    manifest_parser.add_argument("--output", default="chunk_task_manifest.json", help="任务清单输出路径")

    screen_parser = subparsers.add_parser("screen", help="将 waiting_chunk 转成 waiting_markdown，并调用模型做初筛后写入 new_chunk")
    screen_parser.add_argument("waiting_chunk_root", help="存放待筛选 chunk 文件夹的大目录")
    screen_parser.add_argument("--waiting-markdown", default="waiting_markdown", help="初筛前临时 markdown 输出目录")
    screen_parser.add_argument("--new-chunk", default="new_chunk", help="初筛结果为 YES 的 chunk 输出目录")
    screen_parser.add_argument("--results", default="screen_results.json", help="初筛结果 JSON 输出路径")
    screen_parser.add_argument("--model-path", required=True, help="vLLM 模型路径或模型名")
    screen_parser.add_argument("--rule-text", required=True, help="初筛规则文本")
    screen_parser.add_argument("--temperature", type=float, default=0.1, help="采样温度")
    screen_parser.add_argument("--top-p", type=float, default=0.8, help="top-p")
    screen_parser.add_argument("--max-tokens", type=int, default=32, help="最大输出 token 数")
    screen_parser.add_argument("--gpu-memory-utilization", type=float, default=0.63, help="vLLM 显存占用比例")

    rebuild_parser = subparsers.add_parser("rebuild", help="遍历 new_chunk，重组筛选后的 chunk 为 extracted_data markdown")
    rebuild_parser.add_argument("new_chunk_root", help="存放已筛选 chunk 文件夹的大目录")
    rebuild_parser.add_argument("--output", default="extracted_data", help="markdown 输出目录")
    rebuild_parser.add_argument("--separator", default="\n---\n", help="chunk 编号断开时插入的分隔线")

    extract_parser = subparsers.add_parser("extract", help="对 extracted_data markdown 做细筛抽取并输出 extracted_json")
    extract_parser.add_argument("extracted_data_root", help="存放重组 markdown 的目录")
    extract_parser.add_argument("--output", default="extracted_json", help="抽取 JSON 输出目录")
    extract_parser.add_argument("--results", default="extract_results.json", help="细筛结果 JSON 输出路径")
    extract_parser.add_argument("--model-path", required=True, help="vLLM 模型路径或模型名")
    extract_parser.add_argument("--schema-text", required=True, help="细筛抽取 schema 文本")
    extract_parser.add_argument("--temperature", type=float, default=0.1, help="采样温度")
    extract_parser.add_argument("--top-p", type=float, default=0.8, help="top-p")
    extract_parser.add_argument("--max-tokens", type=int, default=4096, help="最大输出 token 数")
    extract_parser.add_argument("--gpu-memory-utilization", type=float, default=0.63, help="vLLM 显存占用比例")

    args = parser.parse_args()

    if args.command == "prepare":
        run_prepare(
            root_o=Path(args.root_o).resolve(),
            extracted_json_root=Path(args.json_root).resolve(),
            waiting_chunk_root=Path(args.waiting_chunk).resolve(),
            manifest_file=Path(args.manifest).resolve(),
            char_limit=args.char_limit,
            dry_run=not args.execute,
        )
    elif args.command == "chunk":
        chunk_all_jsons(
            json_root=Path(args.json_root).resolve(),
            waiting_chunk_root=Path(args.waiting_chunk).resolve(),
            char_limit=args.char_limit,
        )
    elif args.command == "manifest":
        build_manifest(
            waiting_chunk_root=Path(args.waiting_chunk_root).resolve(),
            manifest_file=Path(args.output).resolve(),
        )
    elif args.command == "screen":
        run_screen(
            waiting_chunk_root=Path(args.waiting_chunk_root).resolve(),
            waiting_markdown_root=Path(args.waiting_markdown).resolve(),
            new_chunk_root=Path(args.new_chunk).resolve(),
            results_file=Path(args.results).resolve(),
            model_path=args.model_path,
            rule_text=args.rule_text,
            temperature=args.temperature,
            top_p=args.top_p,
            max_tokens=args.max_tokens,
            gpu_memory_utilization=args.gpu_memory_utilization,
        )
    elif args.command == "rebuild":
        run_rebuild(
            new_chunk_root=Path(args.new_chunk_root).resolve(),
            extracted_data_root=Path(args.output).resolve(),
            separator=args.separator,
        )
    elif args.command == "extract":
        run_extract(
            extracted_data_root=Path(args.extracted_data_root).resolve(),
            extracted_json_root=Path(args.output).resolve(),
            results_file=Path(args.results).resolve(),
            model_path=args.model_path,
            schema_text=args.schema_text,
            temperature=args.temperature,
            top_p=args.top_p,
            max_tokens=args.max_tokens,
            gpu_memory_utilization=args.gpu_memory_utilization,
        )


if __name__ == "__main__":
    main()

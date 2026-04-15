import argparse
from pathlib import Path

from pipeline import run_extract, run_prepare, run_rebuild, run_screen


DEFAULT_SCREEN_RULE_TEXT = (
    "如果文本包含主要会计数据、主要财务指标、分季度财务指标、资产负债表、利润表、现金流量表，"
    "或者包含与以下指标直接相关的数值或表格，则回答 YES：营业收入、营业总收入、净利润、扣非净利润、"
    "每股收益、净资产收益率、总资产、总负债、货币资金、应收账款、存货、营业成本、销售费用、管理费用、"
    "财务费用、研发费用、经营活动产生的现金流量净额、投资活动产生的现金流量净额、筹资活动产生的现金流量净额、"
    "收回投资收到的现金、投资支付的现金、取得借款收到的现金、偿还债务支付的现金。"
    "如果文本主要是风险提示、公司治理、审计套话、社会责任、管理层空泛表述、股东信息、非财务介绍，"
    "且不包含上述可抽取财务数据，则回答 NO。只能回答 YES 或 NO。"
)


DEFAULT_EXTRACT_SCHEMA_TEXT = (
    '{\n'
    '  "doc_id": "文档名",\n'
    '  "stock_code": null,\n'
    '  "stock_abbr": null,\n'
    '  "report_year": null,\n'
    '  "report_period": null,\n'
    '  "facts": [\n'
    '    {\n'
    '      "statement": "core_indicator | balance_sheet | income_sheet | cash_flow_sheet | supplementary",\n'
    '      "scope": "consolidated | parent | unknown",\n'
    '      "metric_std": "标准指标名",\n'
    '      "metric_alias": "原文指标名",\n'
    '      "value": 0,\n'
    '      "value_raw": "原文数值",\n'
    '      "unit": "元 | 万元 | % | 元/股 | 股 | unknown",\n'
    '      "time_role": "current | prior | prior2 | period_end_current | period_end_prior | quarter_q1 | quarter_q2 | quarter_q3 | quarter_q4",\n'
    '      "table_title": "表名或小节名",\n'
    '      "source_chunk": "chunk_n",\n'
    '      "source_text": "原文片段"\n'
    '    }\n'
    '  ]\n'
    '}'
)


def ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def resolve_workspace_paths(workspace: Path) -> dict[str, Path]:
    return {
        "workspace": workspace,
        "collected_json": workspace / "collected_json",
        "waiting_chunk": workspace / "waiting_chunk",
        "waiting_markdown": workspace / "waiting_markdown",
        "new_chunk": workspace / "new_chunk",
        "extracted_data": workspace / "extracted_data",
        "extracted_json": workspace / "extracted_json",
        "manifest_file": workspace / "chunk_task_manifest.json",
        "screen_results": workspace / "screen_results.json",
        "extract_results": workspace / "extract_results.json",
    }


def run_all(
    root_o: Path,
    workspace: Path,
    model_path: str,
    screen_rule_text: str,
    extract_schema_text: str,
    char_limit: int,
    temperature: float,
    top_p: float,
    screen_max_tokens: int,
    extract_max_tokens: int,
    gpu_memory_utilization: float,
    execute_prepare: bool,
    separator: str,
) -> None:
    paths = resolve_workspace_paths(workspace)
    ensure_dirs(
        paths["workspace"],
        paths["collected_json"],
        paths["waiting_chunk"],
        paths["waiting_markdown"],
        paths["new_chunk"],
        paths["extracted_data"],
        paths["extracted_json"],
    )

    run_prepare(
        root_o=root_o,
        extracted_json_root=paths["collected_json"],
        waiting_chunk_root=paths["waiting_chunk"],
        manifest_file=paths["manifest_file"],
        char_limit=char_limit,
        dry_run=not execute_prepare,
    )
    if not execute_prepare:
        print("prepare 当前是 dry-run，流程在提取阶段后停止。若要继续完整流程，请加 --execute-prepare。")
        return

    run_screen(
        waiting_chunk_root=paths["waiting_chunk"],
        waiting_markdown_root=paths["waiting_markdown"],
        new_chunk_root=paths["new_chunk"],
        results_file=paths["screen_results"],
        model_path=model_path,
        rule_text=screen_rule_text,
        temperature=temperature,
        top_p=top_p,
        max_tokens=screen_max_tokens,
        gpu_memory_utilization=gpu_memory_utilization,
    )
    run_rebuild(
        new_chunk_root=paths["new_chunk"],
        extracted_data_root=paths["extracted_data"],
        separator=separator,
    )
    run_extract(
        extracted_data_root=paths["extracted_data"],
        extracted_json_root=paths["extracted_json"],
        results_file=paths["extract_results"],
        model_path=model_path,
        schema_text=extract_schema_text,
        temperature=temperature,
        top_p=top_p,
        max_tokens=extract_max_tokens,
        gpu_memory_utilization=gpu_memory_utilization,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Unified extraction entry: prepare -> screen -> rebuild -> extract."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    all_parser = subparsers.add_parser("all", help="运行完整流程")
    all_parser.add_argument("root_o", help="原始总目录 O")
    all_parser.add_argument("--workspace", default="extract_workspace", help="流程工作目录")
    all_parser.add_argument("--model-path", required=True, help="vLLM 模型路径或模型名")
    all_parser.add_argument("--screen-rule-text", default=DEFAULT_SCREEN_RULE_TEXT, help="初筛规则文本")
    all_parser.add_argument("--extract-schema-text", default=DEFAULT_EXTRACT_SCHEMA_TEXT, help="细筛抽取 schema 文本")
    all_parser.add_argument("--char-limit", type=int, default=6000, help="分块字数阈值")
    all_parser.add_argument("--temperature", type=float, default=0.1, help="采样温度")
    all_parser.add_argument("--top-p", type=float, default=0.8, help="top-p")
    all_parser.add_argument("--screen-max-tokens", type=int, default=32, help="初筛最大输出 token 数")
    all_parser.add_argument("--extract-max-tokens", type=int, default=4096, help="细筛最大输出 token 数")
    all_parser.add_argument("--gpu-memory-utilization", type=float, default=0.63, help="显存占用比例")
    all_parser.add_argument("--execute-prepare", action="store_true", help="实际执行提取和删除 A 目录")
    all_parser.add_argument("--separator", default="\n---\n", help="重组时 chunk 断号插入的分隔线")

    prep_parser = subparsers.add_parser("prepare", help="仅运行提取、分块、manifest")
    prep_parser.add_argument("root_o", help="原始总目录 O")
    prep_parser.add_argument("--workspace", default="extract_workspace", help="流程工作目录")
    prep_parser.add_argument("--char-limit", type=int, default=6000, help="分块字数阈值")
    prep_parser.add_argument("--execute-prepare", action="store_true", help="实际执行提取和删除")

    screen_parser = subparsers.add_parser("screen", help="仅运行初筛")
    screen_parser.add_argument("--workspace", default="extract_workspace", help="流程工作目录")
    screen_parser.add_argument("--model-path", required=True, help="vLLM 模型路径或模型名")
    screen_parser.add_argument("--screen-rule-text", default=DEFAULT_SCREEN_RULE_TEXT, help="初筛规则文本")
    screen_parser.add_argument("--temperature", type=float, default=0.1, help="采样温度")
    screen_parser.add_argument("--top-p", type=float, default=0.8, help="top-p")
    screen_parser.add_argument("--screen-max-tokens", type=int, default=32, help="初筛最大输出 token 数")
    screen_parser.add_argument("--gpu-memory-utilization", type=float, default=0.63, help="显存占用比例")

    rebuild_parser = subparsers.add_parser("rebuild", help="仅运行重组")
    rebuild_parser.add_argument("--workspace", default="extract_workspace", help="流程工作目录")
    rebuild_parser.add_argument("--separator", default="\n---\n", help="重组时 chunk 断号插入的分隔线")

    extract_parser = subparsers.add_parser("extract", help="仅运行细筛抽取")
    extract_parser.add_argument("--workspace", default="extract_workspace", help="流程工作目录")
    extract_parser.add_argument("--model-path", required=True, help="vLLM 模型路径或模型名")
    extract_parser.add_argument("--extract-schema-text", default=DEFAULT_EXTRACT_SCHEMA_TEXT, help="细筛抽取 schema 文本")
    extract_parser.add_argument("--temperature", type=float, default=0.1, help="采样温度")
    extract_parser.add_argument("--top-p", type=float, default=0.8, help="top-p")
    extract_parser.add_argument("--extract-max-tokens", type=int, default=4096, help="细筛最大输出 token 数")
    extract_parser.add_argument("--gpu-memory-utilization", type=float, default=0.63, help="显存占用比例")

    args = parser.parse_args()

    workspace = Path(getattr(args, "workspace", "extract_workspace")).resolve()
    paths = resolve_workspace_paths(workspace)

    if args.command == "all":
        run_all(
            root_o=Path(args.root_o).resolve(),
            workspace=workspace,
            model_path=args.model_path,
            screen_rule_text=args.screen_rule_text,
            extract_schema_text=args.extract_schema_text,
            char_limit=args.char_limit,
            temperature=args.temperature,
            top_p=args.top_p,
            screen_max_tokens=args.screen_max_tokens,
            extract_max_tokens=args.extract_max_tokens,
            gpu_memory_utilization=args.gpu_memory_utilization,
            execute_prepare=args.execute_prepare,
            separator=args.separator,
        )
    elif args.command == "prepare":
        ensure_dirs(paths["workspace"], paths["collected_json"], paths["waiting_chunk"])
        run_prepare(
            root_o=Path(args.root_o).resolve(),
            extracted_json_root=paths["collected_json"],
            waiting_chunk_root=paths["waiting_chunk"],
            manifest_file=paths["manifest_file"],
            char_limit=args.char_limit,
            dry_run=not args.execute_prepare,
        )
    elif args.command == "screen":
        ensure_dirs(paths["workspace"], paths["waiting_markdown"], paths["new_chunk"])
        run_screen(
            waiting_chunk_root=paths["waiting_chunk"],
            waiting_markdown_root=paths["waiting_markdown"],
            new_chunk_root=paths["new_chunk"],
            results_file=paths["screen_results"],
            model_path=args.model_path,
            rule_text=args.screen_rule_text,
            temperature=args.temperature,
            top_p=args.top_p,
            max_tokens=args.screen_max_tokens,
            gpu_memory_utilization=args.gpu_memory_utilization,
        )
    elif args.command == "rebuild":
        ensure_dirs(paths["workspace"], paths["extracted_data"])
        run_rebuild(
            new_chunk_root=paths["new_chunk"],
            extracted_data_root=paths["extracted_data"],
            separator=args.separator,
        )
    elif args.command == "extract":
        ensure_dirs(paths["workspace"], paths["extracted_json"])
        run_extract(
            extracted_data_root=paths["extracted_data"],
            extracted_json_root=paths["extracted_json"],
            results_file=paths["extract_results"],
            model_path=args.model_path,
            schema_text=args.extract_schema_text,
            temperature=args.temperature,
            top_p=args.top_p,
            max_tokens=args.extract_max_tokens,
            gpu_memory_utilization=args.gpu_memory_utilization,
        )


if __name__ == "__main__":
    main()

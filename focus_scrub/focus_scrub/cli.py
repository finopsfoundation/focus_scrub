from __future__ import annotations

import argparse
import json
from pathlib import Path

from focus_scrub.handlers import HandlerConfig, get_column_handlers_for_dataset, list_datasets
from focus_scrub.io import (
    FileFormat,
    discover_focus_files,
    output_path_for_file,
    read_focus_file,
    write_focus_file,
)
from focus_scrub.mapping import MappingCollector, MappingEngine
from focus_scrub.scrub import DataFrameScrub


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="focus-scrub",
        description="Scrub FOCUS input files and write a normalized output set.",
    )
    parser.add_argument(
        "input_path",
        type=Path,
        help="Path to a file or folder containing FOCUS CSV(.gz)/Parquet files.",
    )
    parser.add_argument(
        "output_path",
        type=Path,
        help="Destination folder for scrubbed output files.",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        choices=list_datasets(),
        help="Dataset name to select the correct column scrub handlers.",
    )
    parser.add_argument(
        "--output-format",
        type=FileFormat,
        choices=list(FileFormat),
        default=FileFormat.PARQUET,
        help="Output file format (default: parquet).",
    )
    parser.add_argument(
        "--date-shift-days",
        type=int,
        default=0,
        help="Number of days to add in DateReformat handlers (default: 0).",
    )
    parser.add_argument(
        "--export-mappings",
        type=Path,
        default=None,
        metavar="PATH",
        help="Optional path to write a JSON file with all handler mappings after processing.",
    )
    parser.add_argument(
        "--load-mappings",
        type=Path,
        default=None,
        metavar="PATH",
        help="Optional path to load mappings from a previous run to ensure consistent scrubbing.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    input_path: Path = args.input_path
    output_path: Path = args.output_path
    dataset: str = args.dataset
    output_format: FileFormat = args.output_format
    date_shift_days: int = args.date_shift_days
    export_mappings: Path | None = args.export_mappings
    load_mappings: Path | None = args.load_mappings

    files = discover_focus_files(input_path)
    if not files:
        parser.error("No supported input files found (.csv, .csv.gz, .parquet).")

    collector = MappingCollector() if export_mappings is not None else None
    handler_config = HandlerConfig(date_shift_days=date_shift_days)

    # Load existing mappings if provided
    mapping_engine = None
    if load_mappings is not None:
        if not load_mappings.exists():
            parser.error(f"Mapping file not found: {load_mappings}")
        mapping_data = json.loads(load_mappings.read_text())
        mapping_engine = MappingEngine()
        if "component_mappings" in mapping_data:
            mapping_engine.load_mappings(mapping_data["component_mappings"])
            print(f"Loaded mappings from: {load_mappings}")

    column_handlers, mapping_engine = get_column_handlers_for_dataset(
        dataset, config=handler_config, collector=collector, mapping_engine=mapping_engine
    )
    scrub = DataFrameScrub(column_handlers=column_handlers)

    for input_file in files:
        df = read_focus_file(input_file)
        scrubbed = scrub.scrub(df)
        destination = output_path_for_file(
            input_file=input_file,
            input_root=input_path,
            output_root=output_path,
            output_format=output_format,
        )
        write_focus_file(scrubbed, destination, output_format)
        print(f"Processed: {input_file} -> {destination}")

    print(f"Done. Processed {len(files)} file(s) for dataset '{dataset}'.")

    if export_mappings is not None:
        export_mappings.parent.mkdir(parents=True, exist_ok=True)

        # Build export data with both column mappings and component mappings
        export_data = {
            "column_mappings": {},
            "component_mappings": mapping_engine.get_all_mappings(),
        }

        if collector is not None:
            export_data["column_mappings"] = {
                col: dict(pairs) for col, pairs in collector.to_dict().items()
            }

        export_mappings.write_text(json.dumps(export_data, indent=2))
        print(f"Mappings written to: {export_mappings}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

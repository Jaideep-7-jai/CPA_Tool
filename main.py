#!/usr/bin/env python3
"""CPA Tool command-line entry point.

All Suppression and Mailing requests use the consolidated criteria processor,
regardless of whether they use one criterion or a combination of Age, State,
ZIP, and Gender.  DoorDash remains a dedicated ZIP workflow.
"""

import argparse
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))


def parse_args():
    parser = argparse.ArgumentParser(description="CPA Tool – request processor")
    parser.add_argument("--request-type",  required=True,
                        choices=["Suppression", "Mailing", "Doordash"],
                        help="Type of request")
    parser.add_argument("--criteria-type", required=True,
                        choices=["age", "state", "zips", "gender", "multi"],
                        help="Criteria type")
    parser.add_argument("--comp-type",     required=True,
                        choices=["greater", "less", "between", "include", "exclude"],
                        help="Comparison / inclusion type")
    # Accept one or more channel values: --channel GREEN --channel ORANGE
    # or a single value like --channel ALL
    parser.add_argument("--channel",       required=True,
                        choices=["ALL", "GREEN", "BLUE", "ORANGE", "ARCAMAX", "APPTNESS"],
                        action="append",
                        dest="channels",
                        help="Channel(s) to process. Repeat flag for multiple: --channel GREEN --channel ORANGE")
    parser.add_argument("--output-dir",    required=True,
                        help="Output directory")
    parser.add_argument("--zip-file",      default=None,
                        help="Path to uploaded ZIP codes file")
    parser.add_argument("--request-id",    type=int, default=None,
                        help="DB request ID (required for all processors)")
    return parser.parse_args()


def main():
    args = parse_args()

    criteria = args.criteria_type.lower()
    req_type = args.request_type

    # Normalise: if ALL is present, treat as ["ALL"]; otherwise deduplicate
    channels = list(dict.fromkeys(args.channels))  # preserve order, deduplicate
    if "ALL" in channels:
        channels = ["ALL"]

    if args.request_id is None:
        print("[ERROR] --request-id is required for every request", file=sys.stderr)
        sys.exit(1)

    if req_type == "Doordash":
        if criteria != "zips":
            print("[ERROR] DoorDash supports ZIP criteria only", file=sys.stderr)
            sys.exit(1)
        from Doordash.doordash_zips import process_doordash_zip_request
        process_doordash_zip_request(
            request_id=args.request_id,
            zip_file=args.zip_file,
            channel=channels,
            output_dir=args.output_dir,
        )
        return

    from REQUEST_PROCESSOR.request_processor import process_request
    process_request(
        request_id=args.request_id,
        channel=channels,
        output_dir=args.output_dir,
        zip_file=args.zip_file,
    )


if __name__ == "__main__":
    main()

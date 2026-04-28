"""
CLI entry point.

Usage
-----
    python -m feast.yaml_feature_definition <yaml_path> [-o <output_path>]

Prints generated Python code to stdout, or writes it to *output_path*.
"""

import argparse
import sys

from .codegen import generate_feast_code


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Feast feature definitions from a YAML spec."
    )
    parser.add_argument("yaml_path", help="Path to the YAML feature-definition file.")
    parser.add_argument(
        "-o", "--output",
        metavar="OUTPUT_PATH",
        default=None,
        help="Write generated Python code to this file (default: print to stdout).",
    )
    args = parser.parse_args()
    code = generate_feast_code(args.yaml_path, output_path=args.output)
    if not args.output:
        sys.stdout.write(code)


if __name__ == "__main__":
    main()

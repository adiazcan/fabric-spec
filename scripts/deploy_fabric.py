#!/usr/bin/env python3
"""Deploy Fabric items from the repository workspace folder."""

from __future__ import annotations

import argparse
import sys

from fabric_cicd import FabricWorkspace, publish_all_items


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy Fabric items with fabric-cicd")
    parser.add_argument("--workspace-id", help="Target Fabric workspace GUID")
    parser.add_argument("--workspace-name", help="Target Fabric workspace display name")
    parser.add_argument(
        "--environment",
        default="DEV",
        choices=["DEV", "PROD"],
        help="Parameter environment to apply (default: DEV)",
    )
    parser.add_argument(
        "--repository-directory",
        default="./workspace",
        help="Path to repository workspace folder (default: ./workspace)",
    )
    parser.add_argument(
        "--item-types",
        nargs="+",
        default=["Notebook", "DataPipeline", "Environment", "Lakehouse"],
        help="Fabric item types in scope",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.workspace_id and not args.workspace_name:
        raise SystemExit("Either --workspace-id or --workspace-name is required")

    ws_kwargs = {
        "environment": args.environment,
        "repository_directory": args.repository_directory,
        "item_type_in_scope": args.item_types,
    }
    if args.workspace_id:
        ws_kwargs["workspace_id"] = args.workspace_id
    if args.workspace_name:
        ws_kwargs["workspace_name"] = args.workspace_name

    ws = FabricWorkspace(**ws_kwargs)

    print(
        f"Starting publish_all_items to workspace_id={args.workspace_id} "
        f"workspace_name={args.workspace_name} "
        f"environment={args.environment} repo={args.repository_directory}"
    )
    publish_all_items(ws)
    print("publish_all_items completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

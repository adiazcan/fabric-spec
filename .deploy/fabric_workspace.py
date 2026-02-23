import os

from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items


def main() -> None:
    workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
    if not workspace_id:
        raise ValueError("FABRIC_WORKSPACE_ID environment variable is required")

    environment = os.getenv("FABRIC_ENVIRONMENT", "DEV")

    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=environment,
        repository_directory="./workspace",
        item_type_in_scope=[
            "Notebook",
            "DataPipeline",
            "Environment",
            "SemanticModel",
            "Report",
            "Lakehouse",
        ],
    )

    publish_all_items(target_workspace)
    unpublish_all_orphan_items(target_workspace)


if __name__ == "__main__":
    main()

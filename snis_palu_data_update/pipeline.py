from datetime import datetime
from pathlib import Path

import pandas as pl
from openhexa.sdk import current_run, pipeline, workspace
from utils import (
    configure_logging,
    connect_to_dhis2,
    get_dataset_version_timestamp,
    get_file_from_dataset,
    get_matching_filenames_from_dataset,
    load_configuration,
    read_json_file,
    save_json_file,
    save_logs,
)


@pipeline("snis_palu_data_update")
def snis_palu_data_update():
    """Pipeline to load data from DSNIS to local workspace rdc palu rapports mensuels."""
    pipeline_path = Path(workspace.files_path) / "pipelines" / "snis_palu_data_update"

    # check updated in dataset (shared from DRC DSNIS)
    source_dataset_id = "snis-palu-mensuel-extracts"
    new_version_dt = get_dataset_version_timestamp(dataset_id=source_dataset_id)
    current_run.log_info(f"New dataset version timestamp: {new_version_dt.strftime('%Y%m%d_%H%M')}")
    to_update = should_push_data(
        new_version_dt=new_version_dt, timestamp_path=pipeline_path / "config" / "last_update.json"
    )

    if to_update:
        load_data_from_dataset(dataset_id=source_dataset_id)

        update_last_run_timestamp(
            new_version_dt=new_version_dt,
            timestamp_filename=pipeline_path / "config" / "last_update.json",
        )
    else:
        current_run.log_info("Data is up to date. No update needed.")


def should_push_data(new_version_dt: datetime, timestamp_path: Path) -> bool:
    """Check if new data is available by comparing the latest dataset version timestamp.

    Returns:
        bool: True if an update is needed, False if data is up to date or on error.
    """
    try:
        last_update = read_json_file(timestamp_path)
        last_update_str = last_update.get("LAST_UPDATE", "")
        last_update_dt = datetime.strptime(last_update_str, "%Y%m%d_%H%M") if last_update_str else None
    except Exception as e:
        current_run.log_warning(f"Error reading last update timestamp: Running update by default. Error: {e}")
        return True  # If we can't read the last update, assume we need to update

    return not last_update_dt or new_version_dt > last_update_dt


def load_data_from_dataset(dataset_id: str) -> None:
    """Load data from the dataset and save it to the local workspace."""
    current_run.log_info(f"loading data from dataset: {dataset_id}")


def update_last_run_timestamp(new_version_dt: datetime, timestamp_filename: Path) -> None:
    """Updates the last run timestamp in the JSON file."""
    (timestamp_filename.parent).mkdir(parents=True, exist_ok=True)
    try:
        save_json_file(
            file_path=timestamp_filename,
            contents={"LAST_UPDATE": new_version_dt.strftime("%Y%m%d_%H%M")},
        )
    except Exception as e:
        current_run.log_error(f"Error updating last run timestamp: {e}")
    current_run.log_info(f"Last run timestamp updated to: {new_version_dt.strftime('%Y%m%d_%H%M')}")


if __name__ == "__main__":
    snis_palu_data_update()

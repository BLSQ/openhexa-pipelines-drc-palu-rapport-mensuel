from datetime import datetime
from pathlib import Path

from openhexa.sdk import current_run, pipeline, workspace
from utils import (
    get_dataset_version_timestamp,
    get_file_from_dataset,
    get_matching_filenames_from_dataset,
    read_json_file,
    save_json_file,
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
        try:
            load_data_from_dataset(dataset_id=source_dataset_id, output_path=pipeline_path / "data")
        except Exception as e:
            current_run.log_error(f"Error loading data from dataset: {e}")
            raise

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


def load_data_from_dataset(dataset_id: str, output_path: Path) -> None:
    """Load data from the dataset and save it to the local workspace."""
    current_run.log_info(f"loading data from dataset: {dataset_id}")
    load_pyramid_metadata(dataset_id=dataset_id, output_path=output_path)
    load_extract_files(dataset_id=dataset_id, output_path=output_path)
    load_population_files(dataset_id=dataset_id, output_path=output_path)


def load_pyramid_metadata(
    dataset_id: str, output_path: Path, pyramid_fname: str = "snis_pyramid_metadata.parquet"
) -> None:
    """Load pyramid metadata from the dataset and save it to the local workspace."""
    pyramid_data = get_file_from_dataset(dataset_id=dataset_id, filename=pyramid_fname)
    pyramid_path = output_path / "pyramid"
    pyramid_path.mkdir(parents=True, exist_ok=True)
    pyramid_data.write_parquet(pyramid_path / pyramid_fname)
    current_run.log_info(f"Saved pyramid file: {pyramid_fname} to {pyramid_path}")


def load_extract_files(dataset_id: str, output_path: Path, pattern: str = "palu_extract_*.parquet") -> None:
    """Load extract files from the dataset and save them to the local workspace."""
    extract_fnames = get_matching_filenames_from_dataset(dataset_id=dataset_id, pattern=pattern)
    extracts_path = output_path / "extracts"
    extracts_path.mkdir(parents=True, exist_ok=True)
    for fname in extract_fnames:
        extract_data = get_file_from_dataset(dataset_id=dataset_id, filename=fname)
        extract_data.write_parquet(extracts_path / fname)
        current_run.log_info(f"Saved extract file: {fname} to {extracts_path}")


def load_population_files(dataset_id: str, output_path: Path, pattern: str = "snis_population_*.parquet") -> None:
    """Load population files from the dataset and save them to the local workspace."""
    pop_fnames = get_matching_filenames_from_dataset(dataset_id=dataset_id, pattern=pattern)
    pop_path = output_path / "population"
    pop_path.mkdir(parents=True, exist_ok=True)
    for fname in pop_fnames:
        pop_data = get_file_from_dataset(dataset_id=dataset_id, filename=fname)
        pop_data.write_parquet(pop_path / fname)
        current_run.log_info(f"Saved population file: {fname} to {fname}")


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

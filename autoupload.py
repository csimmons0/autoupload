#!/usr/bin/env python3

import argparse
import concurrent.futures
import functools
import logging
import os
import pathlib
import shutil
import sys
import threading
from typing import Optional, Set

from pydrive.apiattr import ApiResourceList
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive, GoogleDriveFile

DRIVE_VIDEOS_DIR_NAME = "Videos"
DRIVE_FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'
WORKER_COUNT = 2

logger = logging.getLogger(__package__)

@functools.lru_cache()
def get_drive_dir_id(
        drive: GoogleDrive,
        parent_id: Optional[str],
        dirname: str
) -> Optional[str]:
    actual_parent_id = parent_id
    if parent_id is None:
        actual_parent_id = "root"

    matches: ApiResourceList = drive.ListFile(
        {
            "q": f"'{actual_parent_id}' in parents and title='{dirname}' and "
                 f"trashed=false"}
    ).GetList()
    if len(matches) == 0:
        return None

    if len(matches) > 1:
        raise RuntimeError(
            f"Query for parent_id={actual_parent_id}, title={dirname} resulted in a "
            f"list of size {len(matches)}"
        )

    return matches[0]["id"]


def make_drive_dir(drive: GoogleDrive, parent_id: str, dirname: str) -> GoogleDriveFile:
    logger.debug(f"Making drive directory. parent_id={parent_id} dirname={dirname}")
    drive_dir = drive.CreateFile(dict(
        title=dirname,
        parents=[parent_descriptor(parent_id)],
        mimeType=DRIVE_FOLDER_MIME_TYPE
    ))

    drive_dir.Upload()
    return drive_dir


def make_drive_videos_subdir(drive: GoogleDrive, relative_path: pathlib.Path):
    assert not relative_path.is_absolute()

    logger.debug(f"Making videos subdirectory {relative_path}")

    # Get the ID of the Drive videos directory.
    parent_id = get_drive_dir_id(drive, None, DRIVE_VIDEOS_DIR_NAME)
    if not parent_id:
        raise RuntimeError(
            f"Root videos dir named {DRIVE_VIDEOS_DIR_NAME} does not exist")

    # Iterate over each subdirectory in the relative path.
    for subdir in relative_path.parts:
        # Get the folders that already exist in Drive.
        logger.debug(f"Making subdirectory {subdir}")
        drive_subdirs = drive.ListFile(
            {
                'q': f"'{parent_id}' in parents and trashed=false and "
                     f"mimeType='{DRIVE_FOLDER_MIME_TYPE}'"
            }
        ).GetList()

        # Get the existing Drive folder that has the same name as the
        # subdirectory that is part of the relative path.
        drive_subdir = next(
            (drive_subdir for drive_subdir in drive_subdirs
             if drive_subdir['title'] == subdir),
            None
        )

        # If there is not an existing Drive folder that has the same
        # name as the subdirectory, create such a Drive folder.
        if not drive_subdir:
            drive_subdir = make_drive_dir(drive, parent_id, subdir)

        # Move into the Drive folder that matches this iteration's
        # subdirectory.
        parent_id = drive_subdir["id"]

    return parent_id


def parent_descriptor(parent_id):
    return dict(kind="drive#fileLink", id=parent_id)


def should_skip_directory(directory: pathlib.Path) -> bool:
    return is_dotfile(directory)


def should_skip_file(filepath: pathlib.Path) -> bool:
    return is_dotfile(filepath)


def is_dotfile(path: pathlib.Path) -> bool:
    return path.name[0] == "."


def upload_file(drive: GoogleDrive, drive_dir_id: str, uploaded_files_dir: pathlib.Path, filepath: pathlib.Path):
    # Upload the file.
    logger.info("Uploading %s", filepath.name)
    drive_file = drive.CreateFile(
        dict(
            title=filepath.name, parents=[parent_descriptor(drive_dir_id)]
        )
    )

    drive_file.SetContentFile(str(filepath))
    drive_file.Upload()

    # Close the file. See https://github.com/gsuitedevs/PyDrive/issues/129
    drive_file.SetContentFile("nul")

    # Move the file to the uploaded files folder.
    shutil.move(
        filepath,
        uploaded_files_dir.joinpath(filepath.name)
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("local_videos_path")
    parser.add_argument("local_uploaded_videos_path")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    loglevel = logging.INFO
    if args.debug:
        loglevel = logging.DEBUG

    logging.basicConfig(level=loglevel)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_COUNT)
    semaphore = threading.BoundedSemaphore(value=WORKER_COUNT)

    # Create the uploaded videos folder.
    local_uploaded_videos_path = pathlib.Path(args.local_uploaded_videos_path)
    os.makedirs(str(local_uploaded_videos_path), exist_ok=True)

    # Authenticate with Google and get a GoogleDrive object.
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)

    # Walk the local videos folder.
    local_videos_dir = pathlib.Path(args.local_videos_path)
    for directory, subdirs, filenames in os.walk(local_videos_dir):
        subdirs = list(map(pathlib.Path, subdirs))
        # Filter out subdirectories that this routine should skip.
        subdirs[:] = [subdir for subdir in subdirs if not should_skip_directory(subdir)]

        # If this directory contains no files, move to the next directory in the walk.
        # Note that doing so does not cause subdirectories to be skipped.
        if not filenames:
            logger.debug("%s contains no files", directory)
            continue
            
        # Create the directory in Drive.
        directory_relative_path = pathlib.Path(directory).relative_to(local_videos_dir)
        drive_dir_id = make_drive_videos_subdir(drive, directory_relative_path)

        # Create the uploaded files directory.
        uploaded_files_dir = local_uploaded_videos_path.joinpath(directory_relative_path)
        uploaded_files_dir.mkdir(parents=True, exist_ok=True)

        # Get the names of the files that are already in Drive.
        query = \
            f"'{drive_dir_id}' in parents and mimeType!='{DRIVE_FOLDER_MIME_TYPE}' " \
            f"and trashed=false "

        logger.debug(f"Getting existing files with query={query}")
        drive_files: ApiResourceList = drive.ListFile(dict(q=query)).GetList()
        drive_filenames: Set[str] = set(
            drive_file["title"] for drive_file in drive_files
        )

        logger.debug("drive_filenames=%s", drive_filenames)

        # Iterate over the files that aren't already in Drive.
        for filename in set(filenames) - drive_filenames:
            filepath = pathlib.Path(directory, filename)

            # Skip files that should be skipped.
            if should_skip_file(filepath):
                continue

            def upload_this_file():
                try:
                    upload_file(drive, drive_dir_id, uploaded_files_dir, filepath)
                finally:
                    semaphore.release()

            if not semaphore.acquire(timeout=60 * 60 * 6):
                raise RuntimeError("Timed out")

            executor.submit(upload_this_file)

    executor.shutdown()
    return 0


if __name__ == "__main__":
    exit_code = 1
    try:
        exit_code = main()
    except Exception as exception:
        logger.error(exception)
    finally:
        logger.info("Cache info: %s", get_drive_dir_id.cache_info())

    sys.exit(exit_code)

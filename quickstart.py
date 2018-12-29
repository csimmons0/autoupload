#!/usr/bin/env python3

import argparse
import logging
import pathlib
import sys
from typing import Set

from pydrive.apiattr import ApiResource, ApiResourceList
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

DRIVE_VIDEOS_DIR_NAME = "Videos"

logger = logging.getLogger(__package__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("local_videos_path")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    loglevel = logging.INFO
    if args.debug:
        loglevel = logging.DEBUG

    logging.basicConfig(level=loglevel)

    # Authenticate with Google and get a GoogleDrive object.
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)

    # Get the videos dir in Drive.
    matches: ApiResourceList = drive.ListFile(
        {"q": "'root' in parents and title='{}'".format(DRIVE_VIDEOS_DIR_NAME)}
    ).GetList()
    if len(matches) != 1:
        raise RuntimeError(
            "Query for videos dir resulted in a list of size {}".format(len(matches))
        )

    drive_videos_dir: ApiResource = matches[0]
    drive_videos_dir_id = drive_videos_dir["id"]

    # Get the titles of the videos that are already in Drive.
    drive_videos: ApiResourceList = drive.ListFile(
        dict(q="'{}' in parents and trashed=false".format(drive_videos_dir_id))
    ).GetList()
    drive_video_titles: Set[str] = set(video["title"] for video in drive_videos)
    logger.debug("drive_video_titles=%s", drive_video_titles)

    # Upload the local videos that are not already in Drive.
    local_videos_dir = pathlib.Path(args.local_videos_path)
    for video_path in local_videos_dir.iterdir():
        if video_path.is_dir() or video_path.name in drive_video_titles:
            logger.info("Skipping %s", video_path)
            continue

        logger.info("Uploading %s", video_path.name)
        drive_video = drive.CreateFile(
            dict(
                title=video_path.name, parents=[parent_descriptor(drive_videos_dir_id)]
            )
        )
        drive_video.SetContentFile(str(video_path))
        drive_video.Upload()

    return 0


def parent_descriptor(parent_id):
    return dict(kind="drive#fileLink", id=parent_id)


if __name__ == "__main__":
    sys.exit(main())

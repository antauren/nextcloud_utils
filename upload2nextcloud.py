from contextlib import suppress
from pathlib import Path, PosixPath

import requests
from requests import ConnectionError
from requests.exceptions import SSLError
from retry import retry

WEBDAV_URL = "..."


def upload_dir(dir_path: Path, target_dir: PosixPath, user: str, password: str) -> None:
    make_subdirs(target_dir, user, password)

    uploaded_folder = target_dir / dir_path.name

    make_dir_quietly(uploaded_folder, user, password)
    for path in dir_path.rglob("*"):
        if path.is_dir():
            make_dir_quietly(target_dir=uploaded_folder / path.relative_to(dir_path), user=user, password=password)

        if path.is_file():
            upload_file(
                file_path=path,
                target_dir=uploaded_folder / path.parent.relative_to(dir_path),
                user=user,
                password=password,
            )


@retry(exceptions=(SSLError, ConnectionError), tries=5, delay=10, backoff=2, max_delay=600)
def upload_file(file_path: Path, target_dir: PosixPath, user: str, password: str, with_subdirs: bool = False) -> None:
    # https://docs.nextcloud.com/server/latest/user_manual/ru/files/access_webdav.html#accessing-files-using-curl

    if with_subdirs:
        make_subdirs(target_dir, user, password)

    with open(file_path, "rb") as file:
        response = requests.put(
            url=f"{WEBDAV_URL}/{target_dir}/{file_path.name}",
            data=file,
            auth=(user, password),
            timeout=(1, 10),
        )
        response.raise_for_status()


def make_dir_quietly(target_dir: PosixPath, user: str, password: str) -> None:
    with suppress(requests.exceptions.HTTPError):
        make_dir(target_dir, user, password)


@retry(exceptions=(SSLError, ConnectionError), tries=5, delay=10, backoff=2, max_delay=600)
def make_dir(target_dir: PosixPath, user: str, password: str) -> None:
    # https://docs.nextcloud.com/server/latest/user_manual/ru/files/access_webdav.html#accessing-files-using-curl

    response = requests.request(method="MKCOL", url=f"{WEBDAV_URL}/{target_dir}", auth=(user, password))
    response.raise_for_status()


def make_subdirs(target_dir: PosixPath, user: str, password: str) -> None:
    subdirs = PosixPath()
    for subdir in target_dir.parts:
        subdirs = subdirs.joinpath(subdir)
        make_dir_quietly(subdirs, user, password)

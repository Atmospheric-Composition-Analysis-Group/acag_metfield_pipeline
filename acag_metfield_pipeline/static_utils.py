import hashlib
from typing import Union, Callable, Union
from pathlib import Path
import requests
import logging


def download(url: str, file_path: Union[str, Path], check_file: Callable[[str],bool]=None, preprocess_cb: Callable[[str],None]=None, CHUNK_SIZE=8192) -> bool:
    file_path = Path(file_path) if isinstance(file_path, str) else file_path
    check_file = (lambda s: True) if check_file is None else check_file
    file_passed_checks = False
    log = logging.getLogger("luigi-interface")
    log.debug(f"Downloading {url} -> {file_path}")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()  # raise HTTPError
            file_path.parent.mkdir(parents=True, exist_ok=True)  # make parent directory
            #file_size = int(r.headers.get('content-length', 0))
            with open(file_path, 'wb') as f:
                bytes_downloaded = 0
                #pbar = tqdm(total=file_size, unit='iB', unit_scale=True)
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    bytes_downloaded += len(chunk)
                    #pbar.update(len(chunk))
                    f.write(chunk)
        log.debug(f"Finished downloading {url}")
        if preprocess_cb is not None:
            log.debug(f"Preprocessing {url}")
            preprocess_cb(str(file_path))
            log.debug(f"Finished preprocessing {url}")
        log.debug(f"Running post-download checks on {url}")
        file_passed_checks = check_file(str(file_path))
        log.debug(f"Finished post-download checks (OK={file_passed_checks}) on {url}")
    finally:
        if not file_passed_checks:
            log.error(f"Post-download checks on {url} failed. Deleting file.")
            file_path.unlink(missing_ok=True)
        else:
            log.info(f"Successfully downloaded {url}")

    return file_passed_checks


def md5sum(file_path: Union[str, Path], CHUNK_SIZE=8192) -> str:
    file_path = Path(file_path) if isinstance(file_path, str) else file_path
    file_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        chunk = f.read(CHUNK_SIZE)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(CHUNK_SIZE)
    return file_hash.hexdigest()

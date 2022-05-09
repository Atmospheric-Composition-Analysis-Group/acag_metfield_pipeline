import hashlib
from typing import Union, List, Callable, Union
from pathlib import Path
import requests
from pandas import Timestamp, DateOffset
from tqdm import tqdm
import luigi 
import datetime


class DownloadSpec:
    def __init__(self, url_template: str, file_path_template: str, reference_timestamp: str, frequency: DateOffset) -> None:
        self.url_template = url_template
        self.file_path_template = file_path_template
        self.reference_timestamp = Timestamp(reference_timestamp)
        self.frequency = frequency
        first_offset_seconds = ((self.reference_timestamp + self.frequency) - self.reference_timestamp).total_seconds()
        self.frequency_divides_into_days = 24*60*60 % int(first_offset_seconds) == 0

    @staticmethod
    def _get_strings(format: str, timestamps: List[Timestamp]) -> List[str]:
        return [ts.strftime(format) for ts in timestamps]

    def _get_timestamps(self, start: str, end: str):
        start = Timestamp(start)
        end = Timestamp(end)
        if self.frequency_divides_into_days:
            ts = Timestamp.combine(start, self.reference_timestamp.time())
        else:
            ts = self.reference_timestamp
        timestamps = []
        while ts <= end:
            if ts >= start:
                timestamps.append(ts)
            ts += self.frequency
        return timestamps

    def get_list(self, start: Union[str, Timestamp], end: Union[str, Timestamp]):
        timestamps = self._get_timestamps(start, end)
        urls = self._get_strings(self.url_template, timestamps)
        paths = self._get_strings(self.file_path_template, timestamps)
        return [(u, p) for u, p in zip(urls, paths)] 

    def check_file(self, file_path: str) -> bool:
        return True
    
    def preprocess_callback(self, file_path: str) -> None:
        pass


def download(url: str, file_path: Union[str, Path], check_file: Callable[[str],bool]=None, preprocess_cb: Callable[[str],None]=None, CHUNK_SIZE=8192) -> bool:
    file_path = Path(file_path) if isinstance(file_path, str) else file_path
    check_file = (lambda s: True) if check_file is None else check_file
    file_passed_checks = False

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
        if preprocess_cb is not None:
            preprocess_cb(str(file_path))
        file_passed_checks = check_file(str(file_path))
    finally:
        if not file_passed_checks:
            file_path.unlink(missing_ok=True)

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


class DownloadTask(luigi.Task):
    url = luigi.Parameter()
    file_path = luigi.Parameter()
    check_file = luigi.Parameter()
    preprocess_callback = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.file_path)
    
    def run(self):
        download(self.url, self.file_path, self.check_file, self.preprocess_callback)


class DownloadsForDateRange(luigi.WrapperTask):
    start_date = luigi.DateMinuteParameter()
    end_date = luigi.DateMinuteParameter(default=datetime.datetime.now())
    
    def get_download_specs(self) -> List[DownloadSpec]:
        return []

    def requires(self):
        for spec in self.get_download_specs():
            for url, file_path in spec.get_list(str(self.start_date), str(self.end_date)):
                yield DownloadTask(url=url, file_path=file_path, check_file=spec.check_file, preprocess_callback=spec.preprocess_callback)

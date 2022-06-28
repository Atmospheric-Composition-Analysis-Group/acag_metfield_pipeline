from multiprocessing.sharedctypes import Value
import pathlib
import luigi
import acag_metfield_pipeline.static_utils
from datetime import datetime, timedelta, time
import shutil


class DateMinuteTask(luigi.Task):
    date = luigi.DateMinuteParameter()
    root_dir = luigi.Parameter()
    temporal_frequency: timedelta = None
    temporal_offset: time = None
    
    def relpath_strftime_format(self) -> str:
        pass

    @classmethod
    def get_dateminute_range(cls, start: datetime, stop: datetime):
        dateminute_range = []
        search_time = datetime.combine(start.date(), cls.temporal_offset)
        while search_time < start:
            search_time += cls.temporal_frequency
        while search_time <= stop:
            dateminute_range.append(search_time)
            search_time += cls.temporal_frequency

        return dateminute_range


class DownloadBaseTask(luigi.Task):
    def get_url(self) -> str:
        raise NotImplementedError()

    def get_file_path(self) -> str:
        raise NotImplementedError()

    def check_file(self, file_path: str) -> bool:
        return True
    
    def preprocess_callback(self, file_path: str) -> None:
        pass

    def output(self):
        return luigi.LocalTarget(self.get_file_path())

    def run(self):
        acag_metfield_pipeline.static_utils.download(self.get_url(), self.get_file_path(), self.check_file, self.preprocess_callback)


class DownloadTask(DownloadBaseTask):
    url = luigi.Parameter()
    file_path = luigi.Parameter()

    def get_url(self) -> str:
        return self.url

    def get_file_path(self) -> str:
        return self.file_path


class DateMinuteDownloadTask(DateMinuteTask, DownloadBaseTask):
    def url_strftime_format(self) -> str:
        raise NotImplementedError()
    
    def get_url(self) -> str:
        url_format = self.url_strftime_format()
        return url_format.format(date=self.date) 
    
    def get_file_path(self) -> str:
        strftime_format = self.relpath_strftime_format()
        file_path = pathlib.Path(self.root_dir) / strftime_format.format(date=self.date)
        return file_path


class DateMinuteRangeAggregator(luigi.WrapperTask):
    start = luigi.DateMinuteParameter()
    stop = luigi.DateMinuteParameter()
    task_classes = []

    def requires(self):
        for task_class in self.task_classes:
            dm_range = task_class.get_dateminute_range(self.start, self.stop)
            for dm in dm_range:
                yield task_class(date=dm)


class BatchDownload(luigi.WrapperTask):
    root_dir = luigi.Parameter()
    processed_lists_dir = luigi.OptionalParameter()
    url_list = luigi.Parameter()

    file_type = 'text file'

    def convert_url_to_relpath(self, url: str):
        raise NotImplementedError()

    def skip_download(self, url: str):
        return False

    def _read_file(self, fp) -> str:
        match self.file_type:
            case 'text file':
                return fp.read()
            case 'email':
                import email
                return email.message_from_file(fp).get_payload()
        raise ValueError("bad file type")
            

    def requires(self):
        urls = []
        file_paths = []
        with open(self.url_list) as file:
            file_data = self._read_file(file)
        
        for url in file_data.splitlines():
            url = url.rstrip()
            if len(url) == 0 or self.skip_download(url):
                continue
            urls.append(url)
            file_path = pathlib.Path(self.root_dir) / self.convert_url_to_relpath(url)
            file_paths.append(file_path)
        for url, file_path in zip(urls, file_paths):
            yield DownloadTask(url=url, file_path=file_path)
    
    def run(self):
        super().run()
        if self.processed_lists_dir:
            shutil.move(self.url_list, self.processed_lists_dir)

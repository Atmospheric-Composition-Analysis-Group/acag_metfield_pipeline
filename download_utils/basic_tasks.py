import pathlib
import luigi
import download_utils.static_utils
from datetime import datetime, timedelta, time


class DateMinuteTask(luigi.Task):
    date = luigi.DateMinuteParameter()
    root_dir = luigi.Parameter()
    temporal_frequency: timedelta = None
    temporal_offset: time = None
    
    def relpath_strftime_format(self) -> str:
        pass
    
    def output(self):
        strftime_format = self.relpath_strftime_format()
        file_path = pathlib.Path(self.root_dir) / strftime_format.format(date=self.date)
        return luigi.LocalTarget(file_path)

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


class DateMinuteDownloadTask(DateMinuteTask):
    def url_strftime_format(self) -> str:
        pass

    def check_file(self, file_path: str) -> bool:
        return True
    
    def preprocess_callback(self, file_path: str) -> None:
        pass

    def run(self):
        url_format = self.url_strftime_format()
        url = url_format.format(date=self.date)
        path_format = self.relpath_strftime_format()
        file_path = self.output().path
        download_utils.static_utils.download(url, file_path, self.check_file, self.preprocess_callback)

class DateMinuteRangeAggregator(luigi.WrapperTask):
    start = luigi.DateMinuteParameter()
    stop = luigi.DateMinuteParameter()
    root_dir = luigi.Parameter()
    task_classes = []

    def requires(self):
        for task_class in self.task_classes:
            dm_range = task_class.get_dateminute_range(self.start, self.stop)
            for dm in dm_range:
                yield task_class(date=dm, root_dir=self.root_dir)

import pathlib
import luigi
import download_utils.static_utils


class DateMinuteDownloadTask(luigi.Task):
    date = luigi.DateMinuteParameter()
    root_dir = luigi.Parameter()

    def url_strftime_format(self) -> str:
        pass
    
    def relpath_strftime_format(self) -> str:
        pass
    
    def check_file(self, file_path: str) -> bool:
        return True
    
    def preprocess_callback(self, file_path: str) -> None:
        pass
    
    def output(self):
        strftime_format = self.relpath_strftime_format()
        file_path = pathlib.Path(self.root_dir) / strftime_format.format(date=self.date)
        return luigi.LocalTarget(file_path)
    
    def run(self):
        url_format = self.url_strftime_format()
        url = url_format.format(date=self.date)
        path_format = self.relpath_strftime_format()
        file_path = self.output().path
        download_utils.static_utils.download(url, file_path, self.check_file, self.preprocess_callback)

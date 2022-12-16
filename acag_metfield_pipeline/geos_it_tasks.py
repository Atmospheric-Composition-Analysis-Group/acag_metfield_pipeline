import re
import luigi
import xarray as xr
import acag_metfield_pipeline.basic_tasks
import glob


class DownloadGESDISCOrder(acag_metfield_pipeline.basic_tasks.BatchDownload):
    file_type = 'email'
    
    def convert_url_to_relpath(self, url: str):
        pattern = re.compile(r'https://goldsfs1\.gesdisc\.eosdis\.nasa\.gov/data/GEOSIT/(.*)\.hidden/(.*)')
        if not pattern.match(url):
            raise ValueError(f"Unexpected url: {url}")
        return pattern.sub(r'\1\2', url)

    def skip_download(self, url: str):
        return not re.match(r'.*\.nc4$', url)

    def check_file(self, file_path: str) -> bool:
        opened_successfully = False
        try:
            ds = xr.open_dataset(file_path)
            ds.load()
            ds.close()
            opened_successfully = True
        finally:
            pass
        return opened_successfully


class DownloadNewGESDISCOrders(luigi.WrapperTask):
    new_orders_dir = luigi.Parameter()
    processed_orders_dir = luigi.Parameter()

    def requires(self):
        for order in glob.glob(f"{self.new_orders_dir}/*.eml"):
            yield DownloadGESDISCOrder(url_list=order, processed_lists_dir=self.processed_orders_dir)

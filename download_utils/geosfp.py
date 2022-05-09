from pandas import DateOffset
import xarray as xr
from download_utils.utils import DownloadSpec, DownloadsForDateRange
from typing import List
import luigi
from datetime import date
from pathlib import Path


class DownloadSpecGEOSFP(DownloadSpec):
    def __init__(self, collection_long_name: str, download_root: str, keep_vars: List[str]=None) -> None:
        self.keep_vars = keep_vars
        frequency_hours=int(collection_long_name[4])
        frequency =  DateOffset(hours=frequency_hours)
        if collection_long_name[:4] == "inst":
            reference_timestamp = f"2014-01-01 00:00:00"
        else:
            time_offset = "00:30:00" if frequency_hours == 1 else "01:30:00"
            reference_timestamp = f"2014-01-01 {time_offset}"
        
        file_path_template = f"Y%Y/M%m/D%d/GEOS.fp.asm.{collection_long_name}.%Y%m%d_%H%M.V01.nc4"
        url_template = f"https://portal.nccs.nasa.gov/datashare/gmao/geos-fp/das/{file_path_template}"
        super().__init__(url_template, str(Path(download_root)/file_path_template), reference_timestamp, frequency)
    
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

    def preprocess_callback(self, file_path: str) -> None:
        if self.keep_vars is not None:
            ds = xr.open_dataset(file_path, decode_cf=False, mask_and_scale=False, decode_times=False)
            ds.load()
            ds.close()
            ds = ds[self.keep_vars]
            ds.to_netcdf(file_path)


class GEOSFPDownloadsForDateRange(DownloadsForDateRange):
    data_dir = luigi.Parameter()

    def get_download_specs(self) -> List[DownloadSpec]:
        specs = [
            DownloadSpecGEOSFP(download_root=self.data_dir, collection_long_name="tavg1_2d_rad_Nx",
                               keep_vars=["ALBEDO","CLDTOT","LWGNT","SWGDN"]),
        ]

        return specs


if __name__ == '__main__':
    data_dir = 'C:\\Users\\liamb\\ACAG\\operational_downloads\\scratch'
    all_downloads = [
        GEOSFPDownloadsForDateRange(start_date=date(2021, 1, 1), end_date=date(2021, 1, 1), data_dir=data_dir)
    ]
    luigi.build(all_downloads, workers=4, local_scheduler=True)

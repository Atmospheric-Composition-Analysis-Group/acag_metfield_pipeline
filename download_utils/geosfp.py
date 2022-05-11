import xarray as xr
import download_utils.basic_tasks
from typing import List
import luigi
from datetime import datetime, timedelta, time
import re
import pathlib
from fv3_mass_flux_tools.process import create_derived_wind_dataset


class GEOSFPCollection:
    collection_name = None

    @classmethod
    def get_dateminute_range(cls, start: datetime, stop: datetime):
        method_freq_search = re.search(r'^(inst|tavg)_?([0-9])', cls.collection_name)
        if not method_freq_search:
            raise ValueError("Failed to collection frequency and sampling method from long name")
        
        frequency_hours = int(method_freq_search.group(2))
        frequency = timedelta(hours=frequency_hours)
        
        if method_freq_search.group(1) == "inst":
            offset = time(0, 0)
        elif frequency_hours == 1:
            offset = time(0, 30)
        elif frequency_hours == 3:
            offset = time(1, 30)
        else:
            raise ValueError("Unexpected temporal frequency")

        dateminute_range = []
        search_time = datetime.combine(start.date(), offset)
        while search_time < start:
            search_time += frequency
        while search_time <= stop:
            dateminute_range.append(search_time)
            search_time += frequency

        return dateminute_range


class DownloadGEOSFPCollection(download_utils.basic_tasks.DateMinuteDownloadTask, GEOSFPCollection):
    keep_vars = None

    def relpath_strftime_format(self) -> str:
        return "{date:Y%Y/M%m/D%d}/" f"GEOS.fp.asm.{self.collection_name}" ".{date:%Y%m%d_%H%M}.V01.nc4"

    def url_strftime_format(self) -> str:
        return f"https://portal.nccs.nasa.gov/datashare/gmao/geos-fp/das/{self.relpath_strftime_format()}"
    
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


class tavg1_2d_rad_Nx(DownloadGEOSFPCollection):
    collection_name = "tavg1_2d_rad_Nx"
    keep_vars = ["ALBEDO","CLDTOT","LWGNT","SWGDN"]

class tavg_1hr_ctm_c0720_v72(DownloadGEOSFPCollection):
    collection_name = "tavg_1hr_ctm_c0720_v72"

class tavg_1hr_ctmwind_c0720_v72(luigi.Task, GEOSFPCollection):
    collection_name = "tavg_1hr_ctmwind_c0720_v72"
    date = luigi.DateMinuteParameter()
    root_dir = luigi.Parameter()
    grid_data_dir = luigi.Parameter()

    def requires(self):
        yield tavg_1hr_ctm_c0720_v72(date=self.date)

    def output(self) -> luigi.LocalTarget:
        file_path = pathlib.Path(self.root_dir) / f"{self.date:Y%Y/M%m/D%d}/GEOS.fp.asm.{self.collection_name}.{self.date:%Y%m%d_%H%M}.V01.nc4"
        return luigi.LocalTarget(file_path)
    
    def run(self):
        grid_dir = pathlib.Path(self.grid_data_dir)
        grid = xr.open_mfdataset([str(grid_dir / f"c720.tile{n}.nc") for n in range(1,7)], concat_dim='nf', combine='nested')
        tavg_1hr_ctm = xr.open_dataset(str(self.input()[0].path))
        tavg_1hr_winds = create_derived_wind_dataset(tavg_1hr_ctm, grid, change_of_basis='ronchi', disable_pbar=True)
        path_out = str(self.output().path)
        pathlib.Path(path_out).parent.mkdir(parents=True, exist_ok=True)
        tavg_1hr_winds.to_netcdf(path_out)
        pass


class DateRangeAggregator(luigi.WrapperTask):
    start = luigi.DateMinuteParameter()
    stop = luigi.DateMinuteParameter()
    task_classes = [
        tavg1_2d_rad_Nx,
        tavg_1hr_ctm_c0720_v72,
        tavg_1hr_ctmwind_c0720_v72
    ]

    def requires(self):
        for task_class in self.task_classes:
            dm_range = task_class.get_dateminute_range(self.start, self.stop)
            for dm in dm_range:
                yield task_class(date=dm)


# class GEOSFPMassFluxDerivedWinds(luigi.Task):
#     data_dir = luigi.Parameter()
#     mass_flux_dir = luigi.Parameter()
#     timestamp = luigi.DateHourParameter()

#     def output(self):
#         return luigi.LocalTarget(self.file_path)
    
#     def run(self):
#         download(self.url, self.file_path, self.check_file, self.preprocess_callback)

#     def requires(self):
#         return 


if __name__ == '__main__':
    data_dir = 'C:\\Users\\liamb\\ACAG\\operational_downloads\\scratch'
    all_downloads = [
        #GEOSFPDownloadsForDateRange(start_date=date(2022, 5, 1), end_date=date(2022, 5, 1), data_dir=data_dir),
        #GEOSFPMassFluxDownloadsForDateRange(start_date=datetime(2022, 5, 1, 0), end_date=datetime(2022, 5, 1, 0, 59), data_dir=data_dir)
        DateRangeAggregator(start=datetime(2022,5,1,0), stop=datetime(2022,5,1,0,59))
    ]
    luigi.build(all_downloads, workers=1, local_scheduler=True)

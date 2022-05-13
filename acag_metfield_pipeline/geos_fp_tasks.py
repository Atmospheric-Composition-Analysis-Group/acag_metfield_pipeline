import xarray as xr
import acag_metfield_pipeline.basic_tasks
from typing import List
import luigi
from datetime import datetime, timedelta, time
import re
import pathlib
from fv3_mass_flux_tools.process import create_derived_wind_dataset
import numpy as np
import sparselt.esmf
import sparselt.xr


#region "GEOS-FP collection download base classes"

class CollectionDownloadTask(acag_metfield_pipeline.basic_tasks.DateMinuteDownloadTask):
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

class Tavg3CollectionDownloadTask(CollectionDownloadTask):
    temporal_frequency = timedelta(hours=3)
    temporal_offset = time(hour=1, minute=30)

class Tavg1CollectionDownloadTask(CollectionDownloadTask):
    temporal_frequency = timedelta(hours=1)
    temporal_offset = time(minute=30)

class Inst3CollectionDownloadTask(CollectionDownloadTask):
    temporal_frequency = timedelta(hours=3)
    temporal_offset = time(hour=0, minute=0)

class Inst1CollectionDownloadTask(CollectionDownloadTask):
    temporal_frequency = timedelta(hours=1)
    temporal_offset = time(hour=0, minute=0)


#endregion


#region "GEOS-FP 0.25x0.625 native metfield collection classes"

class tavg1_2d_rad_Nx(Tavg1CollectionDownloadTask):
    collection_name = "tavg1_2d_rad_Nx"
    keep_vars = ["ALBEDO","CLDTOT","LWGNT","SWGDN"]

class tavg1_2d_lnd_Nx(Tavg1CollectionDownloadTask):
    collection_name = "tavg1_2d_lnd_Nx"
    keep_vars = ["FRSNO","GRN","GWETROOT","GWETTOP","LAI","PARDF","PARDR","SNODP","SNOMAS"]

class tavg1_2d_flx_Nx(Tavg1CollectionDownloadTask):
    collection_name = "tavg1_2d_flx_Nx"
    keep_vars = ["EFLUX","EVAP","FRSEAICE","HFLUX","PBLH","PRECANV","PRECCON","PRECLSC","PRECSNO","PRECTOT","USTAR","Z0M"]

class tavg1_2d_slv_Nx(Tavg1CollectionDownloadTask):
    collection_name = "tavg1_2d_slv_Nx"
    keep_vars = ["QV2M","SLP","T2M","TO3","TROPPT","TS","U10M","V10M"]

class inst3_3d_asm_Nv(Inst3CollectionDownloadTask):
    collection_name = "inst3_3d_asm_Nv"
    keep_vars = ["PS","QV","T"]

class tavg3_3d_asm_Nv(Tavg3CollectionDownloadTask):
    collection_name = "tavg3_3d_asm_Nv"
    keep_vars = ["OMEGA","QI","QL","RH","U","V"]

class tavg3_3d_cld_Nv(Tavg3CollectionDownloadTask):
    collection_name = "tavg3_3d_cld_Nv"
    keep_vars = ["DTRAIN","TAUCLI","TAUCLW"]

class tavg3_3d_mst_Ne(Tavg3CollectionDownloadTask):
    collection_name = "tavg3_3d_mst_Ne"
    keep_vars = ["CMFMC","PFICU","PFILSAN","PFLCU","PFLLSAN"]

class tavg3_3d_rad_Nv(Tavg3CollectionDownloadTask):
    collection_name = "tavg3_3d_rad_Nv"
    keep_vars = ["CLOUD"]

class tavg3_3d_mst_Nv(Tavg3CollectionDownloadTask):
    collection_name = "tavg3_3d_mst_Nv"
    keep_vars = ["DQRCU","DQRLSAN","REEVAPCN","REEVAPLSAN"]

class tavg3_2d_chm_Nx(Tavg3CollectionDownloadTask):
    collection_name = "tavg3_2d_chm_Nx"
    keep_vars = ["LWI"]


class NativeGEOSFPCollections(acag_metfield_pipeline.basic_tasks.DateMinuteRangeAggregator):
    task_classes = [
        tavg1_2d_rad_Nx,
        tavg1_2d_lnd_Nx,
        tavg1_2d_flx_Nx,
        tavg1_2d_slv_Nx,
        inst3_3d_asm_Nv,
        tavg3_3d_asm_Nv,
        tavg3_3d_cld_Nv,
        tavg3_3d_mst_Ne,
        tavg3_3d_rad_Nv,
        tavg3_3d_mst_Nv,
        tavg3_2d_chm_Nx,
    ]

#endregion


#region "GEOS-FP C720 mass flux collection classes"

class tavg_1hr_ctm_c0720_v72(Tavg1CollectionDownloadTask):
    collection_name = "tavg_1hr_ctm_c0720_v72"

class inst_1hr_ctm_c0720_v72(Inst1CollectionDownloadTask):
    collection_name = "inst_1hr_ctm_c0720_v72"

class MassFluxCollection(acag_metfield_pipeline.basic_tasks.DateMinuteRangeAggregator):
    task_classes = [
        tavg_1hr_ctm_c0720_v72,
        inst_1hr_ctm_c0720_v72,
    ]

#endregion


#region "GEOS-FP C720 derived wind field collection classes"

class tavg_1hr_ctmwind_c0720_v72(acag_metfield_pipeline.basic_tasks.DateMinuteTask):
    collection_name = "tavg_1hr_ctmwind_c0720_v72"
    grid_data_dir = luigi.Parameter()
    temporal_frequency = timedelta(hours=1)
    temporal_offset = time(minute=30)

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

class MassFluxDerivedWindCollection(acag_metfield_pipeline.basic_tasks.DateMinuteRangeAggregator):
    task_classes = [
        tavg_1hr_ctmwind_c0720_v72,
    ]

#endregion

#region "GEOS-FP derived wind field for GC-Classic nested grids"

class nested_regridder(acag_metfield_pipeline.basic_tasks.DateMinuteTask):
    collection_name = None
    shape_out = (None, None)
    regridding_weights_file = luigi.Parameter()
    gridspec_file = luigi.Parameter()
    keep_vars = []

    def relpath_strftime_format(self) -> str:
        return "{date:Y%Y/M%m/D%d}/" f"GEOS.fp.asm.{self.collection_name}" ".{date:%Y%m%d_%H%M}.V01.nc4"

    def run(self):
        regridding_weights = pathlib.Path(self.regridding_weights_file)
        gridspec_file = xr.open_dataset(self.gridspec_file)
        input_file = xr.open_dataset(str(self.input()[0].path))
        transform = sparselt.esmf.load_weights(
            regridding_weights,
            input_dims=[('nf', 'Ydim', 'Xdim'), (6, 720, 720)],
            output_dims=[('lat', 'lon'), self.shape_out],
        )
        ds = sparselt.xr.apply(transform, input_file[self.keep_vars], gridspec_file)
        ds.attrs = dict(
            title='GEOS FP (Forward Processing) Derived Wind Fields',
            institution='Washington University in St. Louis',
            source=f'calculated from GEOS.fp.asm.tavg_1hr_ctm_c0720_v72 data collection using Atmospheric-Composition-Analysis-Group/acag_metfield_pipeline',
            references='https://github.com/LiamBindle/fv3_mass_flux_tools\nhttp://gmao.gsfc.nasa.gov\nhttps://gmao.gsfc.nasa.gov/operations/GEOS5_V1_File_Specification.pdf',
            comment='See https://github.com/LiamBindle/fv3_mass_flux_tools and Atmospheric-Composition-Analysis-Group/acag_metfield_pipeline for details.'
        )
        for varname in self.keep_vars:
            ds[varname].attrs = input_file[varname].attrs
            ds[varname].encoding = dict(
                dtype='float32',
                _FillValue=1.0e15
            )
        path_out = str(self.output().path)
        pathlib.Path(path_out).parent.mkdir(parents=True, exist_ok=True)
        ds.to_netcdf(path_out)


class a1dyn_nested(nested_regridder):
    temporal_frequency = timedelta(hours=1)
    temporal_offset = time(minute=30)
    keep_vars = ['UA', 'VA']

    def requires(self):
        yield tavg_1hr_ctmwind_c0720_v72(date=self.date)

class i1dyn_nested(nested_regridder):
    temporal_frequency = timedelta(hours=1)
    temporal_offset = time(minute=0)
    keep_vars = ['QV', 'PS']

    def requires(self):
        yield inst_1hr_ctm_c0720_v72(date=self.date)

class a1dyn_as(a1dyn_nested):
    collection_name = "A1dyn.0125x015625.AS"
    shape_out = (529, 577)

class i1dyn_as(i1dyn_nested):
    collection_name = "I1dyn.0125x015625.AS"
    shape_out = (529, 577)

class a1dyn_na(a1dyn_nested):
    collection_name = "A1dyn.0125x015625.NA"
    shape_out = (403, 449)

class i1dyn_na(i1dyn_nested):
    collection_name = "I1dyn.0125x015625.NA"
    shape_out = (403, 449)

class a1dyn_eu(a1dyn_nested):
    collection_name = "A1dyn.0125x015625.EU"
    shape_out = (229, 353)

class i1dyn_eu(i1dyn_nested):
    collection_name = "I1dyn.0125x015625.EU"
    shape_out = (229, 353)

class MassFluxDerivedWindCollectionNested(acag_metfield_pipeline.basic_tasks.DateMinuteRangeAggregator):
    task_classes = [
        a1dyn_as,
        i1dyn_as,
        a1dyn_na,
        i1dyn_na,
        a1dyn_eu,
        i1dyn_eu,
    ]

#endregion

class AllGEOSFPTasks(luigi.WrapperTask):
    start = luigi.DateMinuteParameter()
    stop = luigi.DateMinuteParameter(default=datetime.now()-timedelta(hours=12))

    def requires(self):
        yield NativeGEOSFPCollections(start=self.start, stop=self.stop)
        yield MassFluxCollection(start=self.start, stop=self.stop)
        yield MassFluxDerivedWindCollection(start=self.start, stop=self.stop)
        yield MassFluxDerivedWindCollectionNested(start=self.start, stop=self.stop)

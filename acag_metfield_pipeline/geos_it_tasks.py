import re
import luigi
import xarray as xr
import acag_metfield_pipeline.basic_tasks
import glob
from datetime import datetime, timedelta, time


class DownloadGESDISCOrder(acag_metfield_pipeline.basic_tasks.BatchDownload):
    file_type = 'email'
    
    def convert_url_to_relpath(self, url: str):
        pattern = re.compile(r'https://goldsfs1\.gesdisc\.eosdis\.nasa\.gov/data/GEOSIT/(.*)\.hidden/(.*)')
        if not pattern.match(url):
            raise ValueError(f"Unexpected url: {url}")
        return "{date:Y%Y/M%m/D%d}/" f"GEOS.it.asm.{self.collection_name}" ".{date:%Y%m%d_%H%M}.V01.nc4"
        #return pattern.sub(r'\1\2', url)

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

class GEOSIT_ASM_I3_C_V72(DownloadGESDISCOrder):
    collection_name = "GEOSIT_ASM_I3_C_V72.5.29.4"

class GEOSIT_ASM_T3_C_V72(DownloadGESDISCOrder):
    collection_name = "GEOSIT_ASM_T3_C_V72.5.29.4"

class GEOSIT_CHM_T3_C_SLV(DownloadGESDISCOrder):
    collection_name = "GEOSIT_CHM_T3_C_SLV.5.29.4"
    
class GEOSIT_CLD_T3_C_V72(DownloadGESDISCOrder):
    collection_name = "GEOSIT_CLD_T3_C_V72.5.29.4"
    
class GEOSIT_CTM_I1_C_V72(DownloadGESDISCOrder):
    collection_name = "GEOSIT_CTM_I1_C_V72.5.29.4"

class GEOSIT_CTM_T1_C_V72(DownloadGESDISCOrder):
    collection_name = "GEOSIT_CTM_T1_C_V72.5.29.4"

class GEOSIT_FLX_T1_C_SLV(DownloadGESDISCOrder):
    collection_name = "GEOSIT_FLX_T1_C_SLV.5.29.4"
    
class GEOSIT_LFO_T1_C_SLV(DownloadGESDISCOrder):
    collection_name = "GEOSIT_LFO_T1_C_SLV.5.29.4"
    
class GEOSIT_LND_T1_C_SLV(DownloadGESDISCOrder):
    collection_name = "GEOSIT_LND_T1_C_SLV.5.29.4"
    
class GEOSIT_MST_T3_C_V72(DownloadGESDISCOrder):
    collection_name = "GEOSIT_MST_T3_C_V72.5.29.4"
    
class GEOSIT_MST_T3_C_V73(DownloadGESDISCOrder):
    collection_name = "GEOSIT_MST_T3_C_V73.5.29.4"
    
class GEOSIT_RAD_T1_C_SLV(DownloadGESDISCOrder):
    collection_name = "GEOSIT_RAD_T1_C_SLV.5.29.4"
    
class GEOSIT_RAD_T3_C_V72(DownloadGESDISCOrder):
    collection_name = "GEOSIT_RAD_T3_C_V72.5.29.4"
    
class GEOSIT_SLV_T1_C_SLV(DownloadGESDISCOrder):
    collection_name = "GEOSIT_SLV_T1_C_SLV.5.29.4"
    
    
class DownloadNewGESDISCOrders(luigi.WrapperTask):
    new_orders_dir = luigi.Parameter()
    processed_orders_dir = luigi.Parameter()

    def requires(self):
        for order in glob.glob(f"{self.new_orders_dir}/*.eml"):
            yield DownloadGESDISCOrder(url_list=order, processed_lists_dir=self.processed_orders_dir)

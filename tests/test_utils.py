from sqlite3 import Date
from pandas import Timestamp, DateOffset
from download_utils.utils import *
from download_utils.geosfp import tavg1_2d_rad_Nx


def test_download_list():
    list_gen = DownloadSpec("foo%Y%m%d", "bar%m%d", "2022-01-01", DateOffset(months=1))
    assert list_gen.frequency_divides_into_days == False
    result = list_gen._get_timestamps("2022-06-01", "2022-08-01")
    answer = [Timestamp(f"2022-{month:02d}-01") for month in [6, 7, 8]]
    assert  result == answer

    result = list_gen.get_list("2022-06-01", "2022-08-01")

    answer = [
        ("foo20220601", "bar0601"),
        ("foo20220701", "bar0701"),
        ("foo20220801", "bar0801")
    ]

    assert result == answer

def test_tavg1_2d_rad_Nx_download_list():
    spec = tavg1_2d_rad_Nx(download_root="foo")
    answer = [
        (f"https://portal.nccs.nasa.gov/datashare/gmao/geos-fp/das/Y2021/M01/D01/GEOS.fp.asm.tavg1_2d_rad_Nx.20210101_{hour:02d}30.V01.nc4",
         f"foo/Y2021/M01/D01/GEOS.fp.asm.tavg1_2d_rad_Nx.20210101_{hour:02d}30.V01.nc4")
         for hour in range(0,24)
    ]
    assert spec.frequency_divides_into_days
    result = spec.get_list("2021-01-01", "2021-01-01")
    for (answer_url, answer_path), (result_url, result_path) in zip(answer, result):
        assert answer_url == result_url
        assert Path(answer_path) == Path(result_path)


def test_download(tmpdir):
    url = "https://www.python.org/ftp/python/3.10.3/Python-3.10.3.tgz"
    file_path = Path(tmpdir) / "a/couple/levels/down/python.tgz"
    assert not file_path.exists()

    def check_file(file_path):
        check_file.was_called = True
        return md5sum(file_path) == "f276ffcd05bccafe46da023d0a5bb04a"
    check_file.was_called = False

    assert download(url, file_path, check_file) == True
    assert file_path.exists()
    assert check_file.was_called

    # clean up
    file_path.unlink(missing_ok=True)


def test_bad_download(tmpdir):
    url = "https://www.python.org/ftp/python/3.10.3/Python-3.10.3.tgz"
    file_path = Path(tmpdir) / "a/couple/levels/down/python.tgz"
    assert not file_path.exists()  # check that it doesn't exist

    def check_file(file_path):
        check_file.was_called = True
        assert Path(file_path).exists() # check that it exists here
        return md5sum(file_path) == "12345"  # pretend it's a bad md5sum
    check_file.was_called = False

    assert download(url, file_path, check_file) == False
    assert not file_path.exists()  # check that it doesn't exist here
    assert check_file.was_called

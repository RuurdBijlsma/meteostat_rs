* copy tests from python meteostat https://github.com/meteostat/meteostat-python/blob/master/tests/e2e/test_daily.py
* refresh cache possibility
* overdreven veel comments weghalen
* expand get_stations to only get stations that have info for that datetime (is in the json/binfile)

* helper functions to turn frame output into rust structs or vec of rust structs (trait TryFromPolarsRow)
* check if filter performance can be improved
* add trait endpoints to get a single datapoint from a frame
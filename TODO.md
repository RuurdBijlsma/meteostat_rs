* get range of info and return dataframe
* copy tests from python meteostat https://github.com/meteostat/meteostat-python/blob/master/tests/e2e/test_daily.py
* refresh cache possibility
* overdreven veel comments weghalen
* expand get_stations to only get stations that have info for that datetime (is in the json/binfile)

frame functions i want

* Get frame per latlon (date range or just datetime)
    * combine stations into one frame option (sorted by distance)
* Get station(s) per latlon (date range or just datetime)
* Get frame per station
* helper functions to turn frame output into rust structs or vec of rust structs (trait TryFromPolarsRow)
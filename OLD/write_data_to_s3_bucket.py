import requests
import pandas as pd
import numpy as np
import boto3

import sys
import pandas as pd 
import requests

#download data 2018 forward
for year in range(2018,2025):
    for month in range(1, 13):
        if year != 2024 and month >= 11:
            year_str = str(year)
            month_str = str(month).zfill(2)
            url = 'https://s3.amazonaws.com/capitalbikeshare-data/' + year_str + month_str + 'capitalbikeshare-tripdata.zip'
            r = requests.get(url)
            with open("s3://dc-bike-share-data/capitalbikeshare_trip_data" + year_str + month_str + ".csv") as fd:
                fd.write(r.content)


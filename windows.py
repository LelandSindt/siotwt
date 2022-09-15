import requests
import pandas as pd
from datetime import date, datetime, timedelta 
from pyzipcode import ZipCodeDatabase
import logging
from logging.config import dictConfig
from flask import Flask, json, request
from waitress import serve

zcdb = ZipCodeDatabase()


headers = {
    'User-Agent': 'sioawt',
    'From': 'siotwt@shouldiopenthewindowstonight.com',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})

api = Flask(__name__)
api.logger.setLevel(logging.INFO)

@api.after_request
def log_more(response):
    logging.info(str(response.content_length) + ' ' + str(response.status_code) + ' ' + request.base_url) 
    return response 

@api.route('/api/v1/zipcode/<path:zip>', methods=['GET']) 
def v1_zipcode(zip):
    zipCode = zcdb[zip]
    url = 'https://api.weather.gov/points/' + str(zipCode.latitude)  + ',' + str(zipCode.longitude)
    forecasts = requests.get(url, headers=headers, timeout=1)
    hourlyForecast = requests.get(forecasts.json()['properties']['forecastHourly'], headers=headers, timeout=1 )
    df = pd.DataFrame.from_dict(hourlyForecast.json()['properties']['periods'], orient='columns')
    df = df.drop(columns=['icon', 'name', 'endTime', 'number', 'detailedForecast','temperatureTrend','temperatureUnit'])
    df['windSpeed'] = df['windSpeed'].str.split().str[0]
    df['windSpeed'] = pd.to_numeric(df['windSpeed'])
    #Convert to datetime... 
    df['startTime'] = df['startTime'].astype('datetime64[ns]')
    #set date time index
    df.set_index('startTime', inplace=True)
    #set timezone to UTC
    df = df.tz_localize(tz='UTC')
    #convert timzene to match zipcode #
    df = df.tz_convert(tz=(forecasts.json()['properties']['timeZone']))
    today = datetime.now()
    tomorrow = datetime.now()+timedelta(1)
    selection = df.loc[today.strftime('%Y-%m-%d 12:00:00'):tomorrow.strftime('%Y-%m-%d 12:00:00')]['isDaytime']
    nightdf = df.loc[today.strftime('%Y-%m-%d 12:00:00'):tomorrow.strftime('%Y-%m-%d 12:00:00')][-selection]
    coolHours = nightdf.loc[nightdf['temperature'] < 75].shape[0]
    nightHours = nightdf.shape[0]
    if (coolHours/nightHours >= .75):
        openWindows = 'True'
    else:
        openWindows = 'False'
    return openWindows 

if __name__ == '__main__':
    serve(api, host="0.0.0.0", port=8080, threads = 1) 

quit()




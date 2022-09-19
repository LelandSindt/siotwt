import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from datetime import date, datetime, timedelta 
from pyzipcode import ZipCodeDatabase
import logging
from logging.config import dictConfig
from flask import Flask, json, request
from waitress import serve
import os
import time

zcdb = ZipCodeDatabase()

requestsSessoin = requests.Session()

retries = Retry(total=5,
                backoff_factor=0.1,
                status_forcelist=[ 500, 502, 503, 504 ])

requestsSessoin.mount('https://', HTTPAdapter(max_retries=retries))

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

def youShouldOpenTheWindows(zip, verbose):
    resp = {'youShouldOpenTheWindowsTonight': None, 'error': None, 'status': None}

    if not(zip.isdigit()):
        resp['status'] = 404
        resp['error'] = 'zipcode ' + zip + ' not found'
        return resp
    if int(zip) > 99999:
        resp['status'] = 404
        resp['error'] = 'zipcode ' + zip + ' not found'
        return resp
    try:
        zipCode = zcdb[zip]
    except KeyError:
        resp['status'] = 404
        resp['error'] = 'zipcode ' + zip + ' not found'
        return resp

    url = 'https://api.weather.gov/points/' + str(zipCode.latitude)  + ',' + str(zipCode.longitude)
    try:
        forecasts = requestsSessoin.get(url, headers=headers, timeout=5)
    except Exception as e:
        resp['status'] = 500 
        errmsg = 'unable to retrieve forcasts. ' + str(e)
        resp['error'] =  errmsg
        logging.info(errmsg)
        return resp
    if forecasts.status_code != 200:
        resp['status'] = forecasts.status_code 
        resp['error'] = 'unable to retrieve forcasts'
        return resp

    try:
        hourlyForecast = requestsSessoin.get(forecasts.json()['properties']['forecastHourly'], headers=headers, timeout=5)
    except Exception as e:
        resp['status'] = 500 
        errmsg = 'unable to retrieve forcasts. ' + str(e)
        resp['error'] =  errmsg
        return resp
    if hourlyForecast.status_code != 200:
        resp['status'] = hourlyForecast.status_code 
        resp['error'] = 'unable to retrieve hourly forcast'
        return resp

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
    os.environ['TZ'] = forecasts.json()['properties']['timeZone']
    time.tzset()
    today = datetime.now()
    tomorrow = datetime.now()+timedelta(1)
    selection = df.loc[today.strftime('%Y-%m-%d 12:00:00'):tomorrow.strftime('%Y-%m-%d 12:00:00')]['isDaytime']
    nightdf = df.loc[today.strftime('%Y-%m-%d 12:00:00'):tomorrow.strftime('%Y-%m-%d 12:00:00')][-selection]
    coolHours = nightdf.loc[nightdf['temperature'] < 75].shape[0]
    nightHours = nightdf.shape[0]
    if (coolHours/nightHours >= .75):
        resp['youShouldOpenTheWindowsTonight'] = True
    else:
        resp['youShouldOpenTheWindowsTonight'] = False
    if verbose:
        nightdf.index = nightdf.index.strftime('%Y-%m-%dT%H:%M:%S%z')
        resp['data'] = nightdf.to_dict(orient = 'index')
    resp['status'] = 200
    return resp

api = Flask(__name__)
api.logger.setLevel(logging.INFO)

@api.after_request
def log_more(response):
    data = response.get_json() 
    if data:
        response.status_code = data.get('status', response.status_code)
    logging.info(str(response.content_length) + ' ' + str(response.status_code) + ' ' + request.remote_addr + ' ' + request.base_url) 
    return response 

@api.route('/api/v1/zipcode/<path:zip>/verbose', methods=['GET'])
def vi_zipcode_verbose(zip):
    resp = youShouldOpenTheWindows(zip, verbose=True)
    return resp


@api.route('/api/v1/zipcode/<path:zip>', methods=['GET']) 
def v1_zipcode(zip):
    resp = youShouldOpenTheWindows(zip, verbose=False)
    return resp

if __name__ == '__main__':
    serve(api, host="0.0.0.0", port=8080, threads = 1) 

quit()




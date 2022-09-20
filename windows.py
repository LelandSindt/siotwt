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

def youShouldOpenTheWindows(zip, request, verbose):
    resp = {'youShouldOpenTheWindowsTonight': None, 'error': None, 'status': None}
    data = None

    if request.method in ['POST']:
        data = request.json 

    if data is None:
        data = {}

    resp['maxTemp'] = data.get('maxTemp', 75)
    resp['minTemp'] = data.get('minTemp', 55)
    resp['minPercentOfNightBelowMax'] = data.get('minPercentOfNightBelowMax', 75)
    resp['maxPercentOfNightBelowMin'] = data.get('maxPercentOfNightBelowMin', 10)

    if not(isinstance((resp['maxTemp']), int)):
        resp['status'] = 400
        resp['error'] = 'maxTemp must be int'
        return resp

    if not(isinstance((resp['minTemp']), int)):
        resp['status'] = 400
        resp['error'] = 'minTemp must be int'
        return resp

    if not(resp['minTemp'] < resp['maxTemp']):
        resp['status'] = 400
        resp['error'] = 'minTemp must be less than maxTemp'
        return resp

    if not(isinstance((resp['minPercentOfNightBelowMax']), int)):
        resp['status'] = 400
        resp['error'] = 'minPercentOfNightBelowMax must be int'
        return resp

    if not(isinstance((resp['maxPercentOfNightBelowMin']), int)):
        resp['status'] = 400
        resp['error'] = 'maxPercentOfNightBelowMin must be int'
        return resp

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
    coolHours = nightdf.loc[nightdf['temperature'] < resp['maxTemp']].shape[0]
    coldlHours = nightdf.loc[nightdf['temperature'] < resp['minTemp']].shape[0]
    nightHours = nightdf.shape[0]
    resp['coolHoursPercent'] = int(coolHours/nightHours * 100)
    resp['coldHoursPercent'] = int(coldlHours/nightHours * 100)
    if (resp['coolHoursPercent'] >= resp['minPercentOfNightBelowMax'] and resp['coolHoursPercent'] >= resp['maxPercentOfNightBelowMin']):
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
def log_more(response, requst=request):
    data = response.get_json() 
    if data:
        response.status_code = data.get('status', response.status_code)
    logging.info(str(response.content_length) + ' ' + str(response.status_code) + ' ' + request.method + ' ' + request.remote_addr + ' ' + request.base_url) 
    return response 

@api.errorhandler(404)
def page_not_found(error):
    resp = {'youShouldOpenTheWindowsTonight': None, 'error': 'this page does not exist', 'status': 404}
    return resp, 404

@api.errorhandler(500)
def page_not_found(error):
    resp = {'youShouldOpenTheWindowsTonight': None, 'error': 'something went wrong ¯\_(ツ)_/¯ ', 'status': 500}
    return resp, 500 

@api.route('/api/v1/zipcode/<zip>/verbose', methods=['GET', 'POST'])
def vi_zipcode_verbose(zip, request=request):
    resp = youShouldOpenTheWindows(zip, request, verbose=True)
    return resp


@api.route('/api/v1/zipcode/<zip>', methods=['GET', 'POST']) 
def v1_zipcode(zip, request=request):
    resp = youShouldOpenTheWindows(zip, request, verbose=False)
    return resp

if __name__ == '__main__':
    serve(api, host="0.0.0.0", port=8080, threads = 1) 

quit()




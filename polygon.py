import requests
import configparser
import time
import logging

import database as db

def polygonGet(action, url, params):

    '''
    Simply wrapper to check the rate limit of requests going to Polygon
    '''

    while db.queryRateExceeded():
        # First line of defense attempt to catch exceeded rate limits locally before sending a request
        logging.warning('WARNING: Rate limit exceeded (5): Waiting 30 seconds before trying again.')
        time.sleep(30)

    while True:
        response = requests.get(url,params=params)
        db.record(action, url, response.status_code)

        if response.status_code != 429:
            break
        else:
            # This is a second line to repeat a failed request due to rate limits
            logging.warning(f'WARNING: Request failed with status code: {response.status_code}. Trying again in 30 seconds.')
            time.sleep(30)

    if response.status_code == 200:
        return response
    else:
        logging.warning(f'Request failed with status code: {response.status_code}')
        return None

def getClose(ticker, key):

    # Query Builder
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/prev'
    params = {
        'apiKey':key,
        'adjusted':'true'
    }

    response = polygonGet('getClose', url, params)
    data = response.json().get('results')[0]
    return data

if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read('config.cfg')
    db.init(config['POLYGON']['rateDb'])

    close = getClose('VOO', config['POLYGON']['apikey'])
    logging.info(close)


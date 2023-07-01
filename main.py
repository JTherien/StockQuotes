import os
import logging
import configparser

import pandas as pd
import openpyxl

from polygon import getClose

if __name__ == '__main__':

    # Set working directory
    scriptPath = os.path.abspath(__file__)
    dirPath = os.path.dirname(scriptPath)
    os.chdir(dirPath)

    logging.basicConfig(format='>>> %(message)s', level=logging.INFO)

    config = configparser.ConfigParser()
    config.read('config.cfg')

    WORKBOOK = config['EXCEL']['filePath']
    WORKSHEET = config['EXCEL']['sheetName']
    APIKEY = config['POLYGON']['apikey']

    if os.path.exists(WORKBOOK):
        
        # Load security data
        logging.info('Loading financial model.')
        data = pd.read_excel(WORKBOOK, sheet_name=WORKSHEET)
        lastClose = {}

        logging.info(f'Updating stock prices for {data.shape[0]} securities.')

        for ticker in data['Ticker'].unique():
            
            logging.info(f'Fetching: {ticker}')
            polygonData = getClose(ticker, APIKEY)
            lastClose[ticker] =polygonData['c'] 

        data['Market Price'] = data['Ticker'].map(lastClose)

        # Write market prices
        logging.info('Writing last close price to Excel.')
        openpyxl_file = openpyxl.load_workbook(WORKBOOK)
        openpyxl_sheet = openpyxl_file[WORKSHEET]

        for row in range(data.shape[0]):
            openpyxl_sheet[f'F{row+2}'] = data.loc[row, 'Market Price']

        # Save file
        logging.info('Saving updated pricing data.')
        openpyxl_file.save(WORKBOOK)
    
    else:
        logging.info(f'File not found: {WORKBOOK}')

    logging.info('Done.')
import pandas as pd
import os
from sqlalchemy import create_engine
import logging

loggind. basicConfig(
    filename = "logs/ingestion_db.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname) - %(message)s",
    filemode = "a" #append
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''this function will ingest the dataframe into database table'''
    df.to_sql(table_name, con = engine, if_exists = 'append', index = False)

def load_raw_data():
    '''this function will load the CSVS as dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            chunk_iter = pd.read_csv('data/' + file, chunksize=100000)  # adjust size if needed
            for chunk in chunk_iter:
            logging.info(f'Ingesting {file} in db')
                ingest_db(chunk, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info('----------Ingestion Complete----------')
    logging.info(f'Total Time Taken: {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()
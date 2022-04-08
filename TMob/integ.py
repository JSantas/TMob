from fastapi import Response
from fastapi import FastAPI
import config
import cx_Oracle
from pyhive import hive
import pandas as pd

app = FastAPI()


@app.get("/client/{client_id}")
async def return_billings(client_id: str):
    try:
        oracle_conn = cx_Oracle.connect(config.username, config.password, config.dsn)
        oracle_cursor = oracle_conn.cursor()
        query = f'select cislo from CRM where id = "{client_id}"'
        for row in oracle_cursor.execute(query):
            phone_number = row[0]
        hive_conn = hive.Connection(
            host=config.host,
            port=config.port,
            username=config.hive_username)
        hive_query = f'select * from kafka_billings ' \
                     f'where (a_party = {phone_number} or b_party = {phone_number}) and ' \
                     f'_timestamp > 1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval 3 DAYS)'
        dataframe = pd.read_sql(hive_query, hive_conn)
        return dataframe.to_json
    except:
        return {'message' : 'Error'}
    finally:
        if oracle_conn:
            oracle_conn.close()
        if hive_conn:
            hive_conn.close()


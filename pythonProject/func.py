import logging
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from Queries import insert_query, sql_query
from main import connection, csvPath


def check_table(sf_conn):
    sf_cur = sf_conn.cursor()
    try:
        sf_cur.execute(sql_query)
        logging.info(sf_cur.fetchall())
    except Exception as eUploadingFailed:
        logging.info(eUploadingFailed)
        raise eUploadingFailed
    finally:
        sf_cur.close()


def get_data():
    sf_conn = SnowflakeHook(snowflake_conn_id=connection).get_conn()
    check_table(sf_conn=sf_conn)
    sf_cur = sf_conn.cursor()
    for i in pd.read_csv(csvPath, chunksize=10000):
        i[['IOS_App_Id', 'Developer_IOS_Id', 'Total_Average_Rating', 'Total_Number_of_Ratings',
           'Average_Rating_For_Version', 'Number_of_Ratings_For_Version', 'Price_USD']] = i[
            ['IOS_App_Id', 'Developer_IOS_Id', 'Total_Average_Rating', 'Total_Number_of_Ratings',
             'Average_Rating_For_Version', 'Number_of_Ratings_For_Version', 'Price_USD']].fillna(-1)
        i[['Description', 'Languages', 'All_Genres', 'Primary_Genre', 'Current_Version_Release_Date',
           'Original_Release_Date', 'Age_Rating', 'Seller_Official_Website', 'IOS_Store_Url', 'Developer_Name',
           'Title']] = i[['Description', 'Languages', 'All_Genres', 'Primary_Genre', 'Current_Version_Release_Date',
                          'Original_Release_Date', 'Age_Rating', 'Seller_Official_Website', 'IOS_Store_Url',
                          'Developer_Name', 'Title']].fillna("-1")
        df_to_list = i.values.tolist()
        sf_cur.executemany(insert_query, df_to_list)
    sf_cur.close()
    sf_conn.close()

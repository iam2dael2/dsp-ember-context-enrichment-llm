from langchain_community.utilities import SQLDatabase
from sqlalchemy import create_engine
from sqlalchemy.types import *
from typing import List
from commons.checkpoint.google_cloud_console import GoogleCloudStorage
import sqlite3
import pandas as pd
import os

DATABASE_URI: str = 'sqlite:///context_enrichment.db'
DATABASE_NAME: str = 'context_enrichment.db' 

def construct_sql_engine(
    database_uri: str,
    database_name: str,
    gcs_obj: GoogleCloudStorage
) -> SQLDatabase:
    """
    Construct SQLAlchemy Engine, then 'save' this in memory by deleting database locally.

    Parameters
    ----------
        database_uri: str
            Database URI

        database_name: str
            Name of database

        gcs_obj: GoogleCloudStorage
            an object of Google Cloud Storage

    Returns
    ----------
        db: SQLDatabase
                SQLAlchemy Engine to SQLite
    """
    # Check if database exists locally
    if os.path.exists(database_name):
        gcs_obj.upload_cs_file(source_file_name=database_name, destination_file_name=database_name)
    
    else:
        gcs_obj.download_cs_file(file_name=database_name, destination_file_name=database_name)

    # Based on database schema, construct SQLAlchemy Engine
    db = SQLDatabase.from_uri(database_uri, sample_rows_in_table_info=3)
    os.remove(database_name)
    return db

def connect_to_sqlite(
    data_dict: dict,
    gcs_obj: GoogleCloudStorage
) -> SQLDatabase:
    """
    Connect to Local SQLite Database and Obtain SQLAlchemy Engine

    Parameters
    ----------
        data_dict: dict
            dictionary consisting of many data (each metric category)

        gcs_obj: GoogleCloudStorage
            an object of Google Cloud Storage

    Returns
    ----------
        db: SQLDatabase
            SQLAlchemy Engine to SQLite
    """
    # Create SQL engine
    engine = create_engine(DATABASE_URI)

    # Establish a connection with SQLite database server
    sqlite_connection = sqlite3.connect(DATABASE_NAME)
    cursor = sqlite_connection.cursor()

    # Load CSV file as a table of database
    for table_name in data_dict:
        # Drop specific table
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

        # Assign the data to be sent into database
        df: pd.DataFrame = data_dict[table_name]
        df_schema = {column: (Boolean if df[column].dtype == "bool" else Integer if df[column].dtype == "int" else (String(100) if df[column].dtype == 'O' else Float)) for column in df.columns}

        # Write records stored in a DataFrame to a SQL database.
        df.to_sql(
            name=table_name, # name of SQL table
            con=engine, # SQLAlchemy engine
            index=False,
            if_exists="replace",
            dtype=df_schema
        )

        print("Create table \"{table_name}\"".format(table_name=table_name))
    
    # Close the connection
    sqlite_connection.close()

    # Return SQLAlchemy Engine
    db = construct_sql_engine(DATABASE_URI, DATABASE_NAME, gcs_obj=gcs_obj)
    return db
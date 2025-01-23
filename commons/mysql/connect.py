import os
import pandas as pd
import mysql.connector
from mysql.connector.cursor_cext import MySQLCursorAbstract, ProgrammingError
from langchain_community.utilities import SQLDatabase
from sqlalchemy import create_engine
from sqlalchemy.types import *
from commons.mysql.queries import queries
from typing import List

# Use MySQL Connection with `context_enrichment`
HOST: str = os.getenv("HOST")
PORT: str = os.getenv("PORT")
USERNAME: str = os.getenv("USERNAME")
PASSWORD: str = os.getenv("PASSWORD")
DATABASE_NAME: str = "context_enrichment"

DATABASE_URI: str = f'mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE_NAME}'

# Load CSV file as a table of database
def connect_to_mysql(
    data_dict: dict
) -> SQLDatabase:
  """
  Connect to Local MySQL Database and Obtain SQLAlchemy Engine

  Parameters
  ----------
    data_dict: dict
      dictionary consisting of many data (each metric category)

  Returns
  ----------
    db: SQLDatabase
      SQLAlchemy Engine to MySQL
  """
  # Create SQL engine
  engine = create_engine(DATABASE_URI)

  # Establish a connection with MySQL database server
  try:
    mydb = mysql.connector.connect(
      host=HOST,
      user=USERNAME,
      password=PASSWORD,
      database=DATABASE_NAME
    )

    print(f"Database `{DATABASE_NAME}` is trying to connect.")

  except ProgrammingError:
    pass

  else:
    # Make an instance of MySQL Cursor
    mycursor: MySQLCursorAbstract = mydb.cursor()

    # Drop database first, because we specify PRIMARY KEY and FOREIGN KEY constraints
    mycursor.execute(f"DROP DATABASE {DATABASE_NAME};")
    mydb.close()

    print(f"Database `{DATABASE_NAME}` is dropped.")

  finally:
    mydb = mysql.connector.connect(
      host=HOST,
      user=USERNAME,
      password=PASSWORD
    )

    # Make an instance of MySQL Cursor
    mycursor: MySQLCursorAbstract = mydb.cursor()

    # Drop database first, because we specify PRIMARY KEY and FOREIGN KEY constraints
    mycursor.execute(f"CREATE DATABASE {DATABASE_NAME};")
    print(f"Database `{DATABASE_NAME}` is created.")

  for table_name in data_dict:

      # Assign the data to be sent into database
      df: pd.DataFrame = data_dict[table_name]

      # Convert `ae_phone_number` to be string-typed
      try:
          df["ae_phone_number"] = df["ae_phone_number"].astype(str)
      
      except KeyError:
          pass

      finally:
          if table_name == "ringkasan_transaksi_history":
              
              # Define price and trx related columns
              price_related_columns: List[str] = [column for column in data_dict[table_name].columns if data_dict[table_name][column].dtype == 'O' and data_dict[table_name][column].str.contains("Rp").sum() > 0]
              trx_related_columns: List[str] = [column for column in data_dict[table_name].columns if data_dict[table_name][column].dtype == 'O' and data_dict[table_name][column].str.contains("trx").sum() > 0]

              # Replace 'Tidak ada' with value of '0'
              chosen_columns: List[str] = price_related_columns + trx_related_columns
              df[chosen_columns] = df[chosen_columns].replace({"Tidak ada": "0"})

              # Preprocess price related columns
              for column in price_related_columns:
                  df[column] = df[column].str.replace("Rp", "").str.replace(".", "").str.strip().astype(int)
              
              # Preprocess trx related columns
              for column in trx_related_columns:
                  df[column] = df[column].str.replace("trx", "").str.strip().astype(int)

          # Define field's type
          df_schema = {column: (Integer if df[column].dtype == 'int' else (String(100) if df[column].dtype == 'O' else Float)) for column in df.columns}

          # Write records stored in a DataFrame to a SQL database.
          df.to_sql(
              name=table_name, # name of SQL table
              con=engine, # SQLAlchemy engine
              index=False,
              if_exists="replace",
              dtype=df_schema
          )
      
          print("Create table \"{table_name}\"".format(table_name=table_name))

  # Run all specified queries
  mydb = mysql.connector.connect(
  host=HOST,
  user=USERNAME,
  password=PASSWORD,
  database=DATABASE_NAME
  )

  mycursor: MySQLCursorAbstract = mydb.cursor()
  for query in queries:
      mycursor.execute(query)

  # Construct SQLAlchemy Engine
  mydb.close()
  db = SQLDatabase.from_uri(DATABASE_URI, sample_rows_in_table_info=3)
  
  return db
from pandas_gbq.gbq import GenericGBQException
from commons.preprocessing.langchain import llm, answer_llm, ContextEnrichmentFullChain
from commons.preprocessing.langchain import answer_prompt, prompt
from commons.preprocessing.langchain import get_columns_from_sql_result, clean_query
from commons.sqlite.connect import DATABASE_NAME, DATABASE_URI, construct_sql_engine
from langchain_community.utilities import SQLDatabase
from operator import itemgetter
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from google.generativeai.types.generation_types import StopCandidateException
from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from commons.checkpoint.google_cloud_console import GoogleCloudStorage
from typing import List
from datetime import datetime
from dao.google_bigquery import GoogleBigQuery
from credential_accessor import CredentialAccessor
import pandas as pd
import json

from dotenv import load_dotenv
load_dotenv()

from argparse import ArgumentParser

if __name__ == "__main__":
    
    parser = ArgumentParser()
    parser.add_argument('-E', '--env', dest="env", type=str, required=True, help="Working environment.", choices=["dev", "prod"])
    parser.add_argument('-S', '--onserver', dest="onserver", action="store_true", help="Server availability.")
    parser.add_argument('-b', '--bucket', dest="bucket", type=str, required=True, help="Name of bucket")
    parser.add_argument('-r', '--regions', dest="regions", type=str, required=True, help="List of regions to process")
    
    args = vars(parser.parse_args())

    # Initialize parameters
    ENV = args["env"]
    ON_SERVER = args["onserver"]
    BUCKET_NAME = args["bucket"]
    REGIONS = [region.strip() for region in args["regions"].split(",")]
    BQ_TABLE_NAME = "mp_bi.mp_bi_fact_context_enrichment_product_summary"

    # Initialize Google Big Query and Google Cloud Storage
    cr_acc: CredentialAccessor = CredentialAccessor(env=ENV, on_server=ON_SERVER)
    big_query: GoogleBigQuery = GoogleBigQuery(cr_acc.get_attr())
    gcs: GoogleCloudStorage = GoogleCloudStorage(bucket_name=BUCKET_NAME, env=ENV, on_server=ON_SERVER)

    # Initialize SQLAlchemy Engine
    db: SQLDatabase = construct_sql_engine(DATABASE_URI, DATABASE_NAME, gcs_obj=gcs)

    # TODO: 1. Integrate Langchain and MySQL Database
    execute_query = QuerySQLDataBaseTool(db=db)  
    write_query = create_sql_query_chain(llm, db, prompt)

    # Define full chain
    prompt_chain = (
        RunnablePassthrough.assign(
            query=(write_query | clean_query)
        ).assign(
            response=itemgetter("query") | execute_query
        )
        .assign(
            columns=itemgetter("query") | StrOutputParser() | get_columns_from_sql_result
        )
    )

    full_chain: ContextEnrichmentFullChain = ContextEnrichmentFullChain(
        prompt_chain, 
        answer_prompt,
        answer_llm
    )

    # Obtain `detail_mitra` data
    detail_mitra_gsutil_uri: str = f"gs://{BUCKET_NAME}/datasets/detail_mitra.csv"
    detail_mitra: pd.DataFrame = pd.read_csv(detail_mitra_gsutil_uri)

    # Preserve some columns to be able to be processed
    columns_to_preserved: List[str] = ["mitra_id", "nama_mitra", "region_mitra"]
    unique_mitra_df: pd.DataFrame = detail_mitra[columns_to_preserved].drop_duplicates()
    today_date: datetime.date = datetime.today().date()

    # Filter which mitra's product recommendation needed to be summarized
    try:
        # Check which mitra that have been processed, with `snapshot_dt` is current date
        query: str = f"select distinct mitra_id from `{BQ_TABLE_NAME}` where cast(snapshot_dt as date) = current_date()"
        existed_mitra_ids: List[int] = big_query.gbq_read(query)["mitra_id"].tolist()

    except GenericGBQException:
        pass

    else:
        # Exclude existed mitra to not being preprocessed again
        unique_mitra_df: pd.DataFrame = unique_mitra_df[~unique_mitra_df["mitra_id"].isin(existed_mitra_ids)]
    
    finally:
        unique_mitra_df: pd.DataFrame = unique_mitra_df[unique_mitra_df["region_mitra"].isin(REGIONS)]

    # TODO: 2. Save the response in Google Big Query
    for idx, row in unique_mitra_df.iterrows():
        # Generate response from specified mitra
        question: str = f"Berikan produk rekomendasi untuk {row['nama_mitra']} dengan mitra id {row['mitra_id']}."
        inputs, response = full_chain.invoke(question, db=db, mitra_id=row['mitra_id'])
        
        # Report the LLM response's progress
        print(f"\nQuestion: {question}")
        print(f"End Response: {response}")

        # Obtain new row from LLM response
        unique_mitra_row: pd.DataFrame = pd.DataFrame(
            data={
                "snapshot_dt": [today_date],
                "mitra_id": [row["mitra_id"]],
                "nama_mitra": [row["nama_mitra"]],
                "product_summary": [response],
                "source": ["Mystique" if "produk_substitusi" in str(inputs["columns"]) else "GMV Contribution"],
                "llm_metadata": [json.dumps(inputs)]
            }
        )

        # Convert `snapshot_dt` type to be datetime
        unique_mitra_row: pd.DataFrame = unique_mitra_row.astype({"snapshot_dt": "datetime64[ns]"})

        # Push summary of product recommendation to Google Big Query
        big_query.gbq_write(
            dataframe=unique_mitra_row,
            bq_cols=unique_mitra_row.columns.tolist(),
            bq_types=["DATETIME"] + ["INTEGER"] + ["STRING"]*4,
            bq_dst_table=BQ_TABLE_NAME,
            bq_partition_key="snapshot_dt",
            bq_write_disposition = "WRITE_APPEND"
        )

        print("==="*20)
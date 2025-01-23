import pandas as pd
from typing import List
from dotenv import load_dotenv
from argparse import ArgumentParser
from dao.google_bigquery import GoogleBigQuery
from credential_accessor import CredentialAccessor
from commons.sqlite.connect import connect_to_sqlite
from commons.checkpoint.google_cloud_console import GoogleCloudStorage
from commons.preprocessing.context_enrichment import structurize_context_enrichment_data, get_product_recommendation
from commons.preprocessing.import_data import get_context_enrichment_data, get_detail_mitra, get_product_candidates, get_product_substitutes

from dotenv import load_dotenv
load_dotenv()

if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument('-E', '--env', dest="env", type=str, required=True, help="Working environment.", choices=["dev", "prod"])
    parser.add_argument('-S', '--onserver', dest="onserver", action="store_true", help="Server availability.")
    parser.add_argument('-b', '--bucket', dest="bucket", type=str, required=True, help="Name of bucket")
    
    args = vars(parser.parse_args())

    # Initialize all the credentials for Google Cloud Storage
    ENV = args["env"]
    ON_SERVER = args["onserver"]
    BUCKET_NAME = args["bucket"]
    
    cr_acc: CredentialAccessor = CredentialAccessor(env=ENV, on_server=ON_SERVER)
    big_query: GoogleBigQuery = GoogleBigQuery(cr_acc.get_attr())

    gcs: GoogleCloudStorage = GoogleCloudStorage(
        bucket_name=BUCKET_NAME,
        env=ENV,
        on_server=ON_SERVER
    )

    # TODO: 1. Import Data
    context_enrichment_data: pd.DataFrame = get_context_enrichment_data(gcs=gcs)
    data: dict = structurize_context_enrichment_data(context_enrichment_data=context_enrichment_data)

    # TODO: 2. Data Preprocessing
    detail_mitra: pd.DataFrame = get_detail_mitra(gcs=gcs) # detail mitra
    product_recommendation: pd.DataFrame = get_product_recommendation(product_recommendation=data["product_recom"]["rekomendasi_produk"], detail_mitra=detail_mitra[["mitra_id", "region_mitra"]], gcs=gcs) # product recommendation
    product_substitution: pd.DataFrame = get_product_substitutes(gcs=gcs) # product substitution
    product_candidates: pd.DataFrame = get_product_candidates(gcs=gcs) # product candidates

    # TODO: 3. Connect to SQLite Database
    data: dict = {
        "detail_mitra": detail_mitra,
        "rekomendasi_produk": product_recommendation,
        "substitusi_produk": product_substitution,
        "kandidat_produk": product_candidates
    }
    
    db = connect_to_sqlite(data, gcs_obj=gcs)
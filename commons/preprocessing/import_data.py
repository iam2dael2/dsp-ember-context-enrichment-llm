import pandas as pd
from dao.google_bigquery import GoogleBigQuery
from credential_accessor import CredentialAccessor
from gcsfs.retry import HttpError
from typing import List, Tuple
from commons.checkpoint.google_cloud_console import GoogleCloudStorage
from datetime import datetime

def get_context_enrichment_data(
    gcs: GoogleCloudStorage
) -> pd.DataFrame:
    """
    Get current context enrichment data

    Parameters
    ----------
        gcs: GoogleCloudStorage
            Google Cloud Storage object

    Returns
    ----------
        context_enrichment_df: pd.DataFrame
            context enrichment data
    """ 
    try:
        file_path: str = "datasets/context_enrichment.csv"
        gsutil_uri: str = f"gs://{gcs.bucket_name}/{file_path}"
        sample_df: pd.DataFrame = pd.read_csv(gsutil_uri)

    except (HttpError, FileNotFoundError):
        cr_acc: CredentialAccessor = CredentialAccessor(
            env=gcs.env,
            on_server=gcs.on_server
        )

        big_query: GoogleBigQuery = GoogleBigQuery(
            attr=cr_acc.get_attr()
        )

        # Write a query sample
        query_file_path: str = "queries/get_context_enrichment_data.sql"
        with open(query_file_path) as query_file:
            sample_query = query_file.read()

        # Read big query into pd.DataFrame
        sample_df: pd.DataFrame = big_query.gbq_read(query=sample_query)
        gcs.get_blob(file_path).upload_from_string(sample_df.to_csv(index=False), "text/csv")

    else:
        # Compare file created date with current date
        file_created_date: datetime.date = gcs.get_file_created_date(file_path)
        current_date: datetime.date = datetime.today().date()

        if file_created_date != current_date:
            cr_acc: CredentialAccessor = CredentialAccessor(
                env=gcs.env,
                on_server=gcs.on_server
            )

            big_query: GoogleBigQuery = GoogleBigQuery(
                attr=cr_acc.get_attr()
            )

            # Write a query sample
            query_file_path: str = "queries/get_context_enrichment_data.sql"
            with open(query_file_path) as query_file:
                sample_query = query_file.read()

            # Read big query into pd.DataFrame
            sample_df: pd.DataFrame = big_query.gbq_read(query=sample_query)
            gcs.get_blob(file_path).upload_from_string(sample_df.to_csv(index=False), "text/csv")

    finally:
        # Remove column `prc_dt`
        sample_df = sample_df.drop("prc_dt", axis=1, errors="ignore")

        # Fill missing values
        sample_df.fillna({"value": "Tidak ada"}, inplace=True)

    return sample_df


def get_purchase_prob_data(
    gcs: GoogleCloudStorage
) -> pd.DataFrame:
    """
    Obtain purchase probability of mitra to specific product data.

    Parameters
    ----------
        gcs: GoogleCloudStorage
            Google Cloud Storage object

    Returns
    ----------
        prob_data: pd.DataFrame
            purchase probability data
    """
    try:
        file_path: str = "datasets/purchase_probs.csv"
        gsutil_uri: str = f"gs://{gcs.bucket_name}/{file_path}" 
        prob_data = pd.read_csv(gsutil_uri)

    except (HttpError, FileNotFoundError):
        cr_acc: CredentialAccessor = CredentialAccessor(
            env=gcs.env,
            on_server=gcs.on_server
        )

        big_query: GoogleBigQuery = GoogleBigQuery(
            attr=cr_acc.get_attr()
        )

        # Write a query sample
        query_file_path: str = "queries/get_propensity_to_buy.sql"
        with open(query_file_path) as query_file:
            sample_query = query_file.read()

        # Read big query into pd.DataFrame
        prob_data: pd.DataFrame = big_query.gbq_read(query=sample_query)
        gcs.get_blob(file_path).upload_from_string(prob_data.to_csv(index=False), "text/csv")

    else:
        # Compare file created date with current date
        file_created_date: datetime.date = gcs.get_file_created_date(file_path)
        current_date: datetime.date = datetime.today().date()

        if file_created_date != current_date:
            cr_acc: CredentialAccessor = CredentialAccessor(
                env=gcs.env,
                on_server=gcs.on_server
            )

            big_query: GoogleBigQuery = GoogleBigQuery(
                attr=cr_acc.get_attr()
            )

            # Write a query sample
            query_file_path: str = "queries/get_propensity_to_buy.sql"
            with open(query_file_path) as query_file:
                sample_query = query_file.read()

            # Read big query into pd.DataFrame
            prob_data: pd.DataFrame = big_query.gbq_read(query=sample_query)
            gcs.get_blob(file_path).upload_from_string(prob_data.to_csv(index=False), "text/csv")
    
    return prob_data

def get_detail_mitra(
    gcs: GoogleCloudStorage
) -> pd.DataFrame:
    """
    Get current context enrichment data

    Parameters
    ----------
        gcs: GoogleCloudStorage
            Google Cloud Storage object

    Returns
    ----------
        detail_mitra: pd.DataFrame
            data of detail mitra
    """
    try:
        file_path: str = "datasets/detail_mitra.csv"
        gsutil_uri: str = f"gs://{gcs.bucket_name}/{file_path}"
        detail_mitra: pd.DataFrame = pd.read_csv(gsutil_uri)

    except (HttpError, FileNotFoundError):
        cr_acc: CredentialAccessor = CredentialAccessor(
            env=gcs.env,
            on_server=gcs.on_server
        )

        big_query: GoogleBigQuery = GoogleBigQuery(
            attr=cr_acc.get_attr()
        )

        # Write a query sample
        query_file_path: str = "queries/get_detail_mitra.sql"
        with open(query_file_path) as query_file:
            sample_query = query_file.read()

        # Read big query into pd.DataFrame
        detail_mitra: pd.DataFrame = big_query.gbq_read(query=sample_query)
        gcs.get_blob(file_path).upload_from_string(detail_mitra.to_csv(index=False), "text/csv")

    else:
        # Compare file created date with current date
        file_created_date: datetime.date = gcs.get_file_created_date(file_path)
        current_date: datetime.date = datetime.today().date()

        if file_created_date != current_date:
            cr_acc: CredentialAccessor = CredentialAccessor(
                env=gcs.env,
                on_server=gcs.on_server
            )

            big_query: GoogleBigQuery = GoogleBigQuery(
                attr=cr_acc.get_attr()
            )

            # Write a query sample
            query_file_path: str = "queries/get_detail_mitra.sql"
            with open(query_file_path) as query_file:
                sample_query = query_file.read()

            # Read big query into pd.DataFrame
            detail_mitra: pd.DataFrame = big_query.gbq_read(query=sample_query)
            gcs.get_blob(file_path).upload_from_string(detail_mitra.to_csv(index=False), "text/csv")
    
    return detail_mitra

def get_smrm_data(
    gcs: GoogleCloudStorage
) -> pd.DataFrame:
    """
    Obtain SMRM data.

    Parameters
    ----------
        gcs: GoogleCloudStorage
            Google Cloud Storage object

    Returns
    ----------
        smrm_data: pd.DataFrame
            smrm data
    """
    try:
        file_path: str = "datasets/smrm_data.csv"
        gsutil_uri: str = f"gs://{gcs.bucket_name}/{file_path}" 
        smrm_data: pd.DataFrame = pd.read_csv(gsutil_uri)

    except (HttpError, FileNotFoundError):
        cr_acc: CredentialAccessor = CredentialAccessor(
            env=gcs.env,
            on_server=gcs.on_server
        )

        big_query: GoogleBigQuery = GoogleBigQuery(
            attr=cr_acc.get_attr()
        )

        # Write a query sample
        query_file_path: str = "queries/get_smrm_products.sql"
        with open(query_file_path) as query_file:
            sample_query = query_file.read()

        # Read big query into pd.DataFrame
        smrm_data: pd.DataFrame = big_query.gbq_read(query=sample_query)
        gcs.get_blob(file_path).upload_from_string(smrm_data.to_csv(index=False), "text/csv")

    else:
        # Compare file created date with current date
        file_created_date: datetime.date = gcs.get_file_created_date(file_path)
        current_date: datetime.date = datetime.today().date()

        if file_created_date != current_date:
            cr_acc: CredentialAccessor = CredentialAccessor(
                env=gcs.env,
                on_server=gcs.on_server
            )

            big_query: GoogleBigQuery = GoogleBigQuery(
                attr=cr_acc.get_attr()
            )

            # Write a query sample
            query_file_path: str = "queries/get_smrm_products.sql"
            with open(query_file_path) as query_file:
                sample_query = query_file.read()

            # Read big query into pd.DataFrame
            smrm_data: pd.DataFrame = big_query.gbq_read(query=sample_query)
            gcs.get_blob(file_path).upload_from_string(smrm_data.to_csv(index=False), "text/csv")
            
    finally:
        # Normalize product name
        smrm_data["nama_produk"] = smrm_data["nama_produk"].apply(lambda product: product.upper().strip())

    return smrm_data

def get_gmv_data(
    gcs: GoogleCloudStorage
) -> pd.DataFrame:
    """
    Obtain GMV data.

    Parameters
    ----------
        gcs: GoogleCloudStorage
            Google Cloud Storage object

    Returns
    ----------
        gmv_data: pd.DataFrame
            GMV data
    """
    try:
        file_path: str = "datasets/gmv_data.csv"
        gsutil_uri: str = f"gs://{gcs.bucket_name}/{file_path}" 
        gmv_data: pd.DataFrame = pd.read_csv(gsutil_uri)

    except (HttpError, FileNotFoundError):
        cr_acc: CredentialAccessor = CredentialAccessor(
            env=gcs.env,
            on_server=gcs.on_server
        )

        big_query: GoogleBigQuery = GoogleBigQuery(
            attr=cr_acc.get_attr()
        )

        # Write a query sample
        query_file_path: str = "queries/get_product_gmv.sql"
        with open(query_file_path) as query_file:
            sample_query = query_file.read()

        # Read big query into pd.DataFrame
        gmv_data: pd.DataFrame = big_query.gbq_read(query=sample_query)
        gcs.get_blob(file_path).upload_from_string(gmv_data.to_csv(index=False), "text/csv")

    else:
        # Compare file created date with current date
        file_created_date: datetime.date = gcs.get_file_created_date(file_path)
        current_date: datetime.date = datetime.today().date()

        if file_created_date != current_date:
            cr_acc: CredentialAccessor = CredentialAccessor(
                env=gcs.env,
                on_server=gcs.on_server
            )

            big_query: GoogleBigQuery = GoogleBigQuery(
                attr=cr_acc.get_attr()
            )

            # Write a query sample
            query_file_path: str = "queries/get_product_gmv.sql"
            with open(query_file_path) as query_file:
                sample_query = query_file.read()

            # Read big query into pd.DataFrame
            gmv_data: pd.DataFrame = big_query.gbq_read(query=sample_query)
            gcs.get_blob(file_path).upload_from_string(gmv_data.to_csv(index=False), "text/csv")
            
    finally:
        # Normalize product name
        gmv_data["nama_produk"] = gmv_data["nama_produk"].apply(lambda product: product.upper().strip())

    return gmv_data
        
def get_product_substitutes(
    gcs: GoogleCloudStorage
) -> pd.DataFrame:
    """
    Obtain product substitutes.

    Parameters
    ----------
        gcs: GoogleCloudStorage
            Google Cloud Storage object

    Returns
    ----------
        product_substitutes: pd.DataFrame
            product substitutes
    """
    try:
        file_path: str = "datasets/product_substitution.csv"
        gsutil_uri: str = f"gs://{gcs.bucket_name}/{file_path}" 
        product_substitution = pd.read_csv(gsutil_uri)

    except (HttpError, FileNotFoundError):
        cr_acc: CredentialAccessor = CredentialAccessor(
            env=gcs.env,
            on_server=gcs.on_server
        )

        big_query: GoogleBigQuery = GoogleBigQuery(
            attr=cr_acc.get_attr()
        )

        # Write a query sample
        query_file_path: str = "queries/get_product_substitutes_from_mystique.sql"
        with open(query_file_path) as query_file:
            sample_query = query_file.read()

        # Read big query into pd.DataFrame
        product_substitution_temp: pd.DataFrame = big_query.gbq_read(query=sample_query)
        product_substitution: pd.DataFrame = pd.DataFrame(columns=product_substitution_temp.columns)

        # Filter product substitutes with better margin
        region_base_prd_pairs: List[Tuple[str]] = list(set(zip(product_substitution_temp["region"], product_substitution_temp["produk_awal"])))

        for region, base_prd in region_base_prd_pairs:
            # If there is product with lower margin, we will ignore these products
            sub_product_substitution: pd.DataFrame = product_substitution_temp[(product_substitution_temp["region"] == region) & (product_substitution_temp["produk_awal"] == base_prd)]
            if False in sub_product_substitution["is_better_margin"].unique():
                sub_product_substitution[["produk_substitusi", "pemasok_produk_substitusi", "harga_produk_substitusi", "bahan_aktif_substitusi"]] = None

            product_substitution: pd.DataFrame = pd.concat([product_substitution, sub_product_substitution])

        # Structurize the corresponding table
        product_substitution["produk_awal"] = product_substitution["produk_awal"].apply(lambda product: product.upper().strip() if type(product) == str else None)
        product_substitution["produk_substitusi"] = product_substitution["produk_substitusi"].apply(lambda product: product.upper().strip() if type(product) == str else None)
        product_substitution.reset_index(drop=True, inplace=True)
        gcs.get_blob(file_path).upload_from_string(product_substitution.to_csv(index=False), "text/csv")

    else:
        # Compare file created date with current date
        file_created_date: datetime.date = gcs.get_file_created_date(file_path)
        current_date: datetime.date = datetime.today().date()

        if file_created_date != current_date:
            cr_acc: CredentialAccessor = CredentialAccessor(
                env=gcs.env,
                on_server=gcs.on_server
            )

            big_query: GoogleBigQuery = GoogleBigQuery(
                attr=cr_acc.get_attr()
            )

            # Write a query sample
            query_file_path: str = "queries/get_product_substitution_from_mystique.sql"
            with open(query_file_path) as query_file:
                sample_query = query_file.read()

            # Read big query into pd.DataFrame
            product_substitution_temp: pd.DataFrame = big_query.gbq_read(query=sample_query)
            product_substitution: pd.DataFrame = pd.DataFrame(columns=product_substitution_temp.columns)

            # Filter product substitutes with better margin
            region_base_prd_pairs: List[Tuple[str]] = list(set(zip(product_substitution_temp["region"], product_substitution_temp["produk_awal"])))

            for region, base_prd in region_base_prd_pairs:
                # If there is product with lower margin, we will ignore these products
                sub_product_substitution: pd.DataFrame = product_substitution_temp[(product_substitution_temp["region"] == region) & (product_substitution_temp["produk_awal"] == base_prd)]
                if False in sub_product_substitution["is_better_margin"].unique():
                    sub_product_substitution[["produk_substitusi", "pemasok_produk_substitusi", "harga_produk_substitusi", "bahan_aktif_substitusi"]] = None

                product_substitution: pd.DataFrame = pd.concat([product_substitution, sub_product_substitution])

            # Structurize the corresponding table
            product_substitution["produk_awal"] = product_substitution["produk_awal"].apply(lambda product: product.upper().strip() if type(product) == str else None)
            product_substitution["produk_substitusi"] = product_substitution["produk_substitusi"].apply(lambda product: product.upper().strip() if type(product) == str else None)
            product_substitution.reset_index(drop=True, inplace=True)
            gcs.get_blob(file_path).upload_from_string(product_substitution.to_csv(index=False), "text/csv")

    return product_substitution

def get_product_candidates(
    gcs: GoogleCloudStorage
):
    """
    Obtain product candidates, if mitra has no recommended products.

    Parameters
    ----------
        gcs: GoogleCloudStorage
            Google Cloud Storage object

    Returns
    ----------
        product_candidates: pd.DataFrame
            product candidates
    """
    try:
        file_path: str = "datasets/product_candidates.csv"
        gsutil_uri: str = f"gs://{gcs.bucket_name}/{file_path}" 
        product_candidates: pd.DataFrame = pd.read_csv(gsutil_uri)

    except (HttpError, FileNotFoundError):
        cr_acc: CredentialAccessor = CredentialAccessor(
            env=gcs.env,
            on_server=gcs.on_server
        )

        big_query: GoogleBigQuery = GoogleBigQuery(
            attr=cr_acc.get_attr()
        )

        # Write a query sample
        query_file_path: str = "queries/get_big_frac_gmv_products.sql"
        with open(query_file_path) as query_file:
            sample_query = query_file.read()

        # Read big query into pd.DataFrame
        product_candidates: pd.DataFrame = big_query.gbq_read(query=sample_query)
        gcs.get_blob(file_path).upload_from_string(product_candidates.to_csv(index=False), "text/csv")

    else:
        # Compare file created date with current date
        file_created_date: datetime.date = gcs.get_file_created_date(file_path)
        current_date: datetime.date = datetime.today().date()

        if file_created_date != current_date:
            cr_acc: CredentialAccessor = CredentialAccessor(
                env=gcs.env,
                on_server=gcs.on_server
            )

            big_query: GoogleBigQuery = GoogleBigQuery(
                attr=cr_acc.get_attr()
            )

            # Write a query sample
            query_file_path: str = "queries/get_big_frac_gmv_products.sql"
            with open(query_file_path) as query_file:
                sample_query = query_file.read()

            # Read big query into pd.DataFrame
            product_candidates: pd.DataFrame = big_query.gbq_read(query=sample_query)
            gcs.get_blob(file_path).upload_from_string(product_candidates.to_csv(index=False), "text/csv")

    return product_candidates
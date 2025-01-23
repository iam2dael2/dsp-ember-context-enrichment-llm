import pandas as pd
import functools as ft
from typing import List, Union, Any, Tuple
from commons.checkpoint.google_cloud_console import GoogleCloudStorage
from commons.preprocessing.import_data import get_smrm_data, get_gmv_data

def structurize_context_enrichment_data(
    context_enrichment_data: pd.DataFrame
) -> dict:
    """
    Preprocess context enrichment data to be more structured

    Parameters
    ----------
        context_enrichment_data: pd.DataFrame
            specified context enrichment data

    Returns
    ----------
        context_enrichment_dict: dict
            structued context enrichment data
    """
    data: dict = dict()
    table_categories: List[str] = context_enrichment_data["table_category"].unique()

    for table_category in table_categories:
        # List all tables
        table_category_df: pd.DataFrame = context_enrichment_data[context_enrichment_data["table_category"] == table_category]
        metric_categories: List[str] = table_category_df["metric_category"].unique()
        data[table_category] = dict()

        for metric_category in metric_categories:
            # List all metric categories
            metric_category_df: pd.DataFrame = table_category_df[table_category_df["metric_category"] == metric_category]
            data[table_category][metric_category] = metric_category_df.reset_index(drop=True)

    return data

def modify_data(data: pd.DataFrame):
    """
    Obtain modified data to be more structured

    Parameters
    ----------
        data: pd.DataFrame
            specified data

    Returns
    ----------
        modified_data: dict
            modified data
    """
    def unmelt_df(
        melted_df: pd.DataFrame,
        index: Union[str, List[str]],
        columns: Union[str, List[str]]
    ) -> dict:
        """
        Unmelt the melted data

        Parameters
        ----------
            melted_df: pd.DataFrame
                specified data

            columns : str or list of str
                columns to use to make new frame's columns.

            index : str or list of str
                columns to use to make new frame's index.

        Returns
        ----------
            result_dict: dict
                pairs of metric category and corresponding data
        """
        unmelted_df: pd.DataFrame = melted_df.pivot(index=index, columns=columns)
        unmelted_df = unmelted_df['value'].reset_index()
        unmelted_df.columns.name = None
        return unmelted_df
    
    # Assign values to be able to extract
    data["value"] = data["value"].str.split(" ; ")
    data = data.drop_duplicates(subset=["mitra_id", "metric_name"])

    # Unmelt the data
    data = unmelt_df(data, index=["mitra_id", "snapshot_dt"], columns=["metric_name"])
    columns_to_explode: List[str] = data.columns.tolist()[2:]
    data = data.explode(columns_to_explode).reset_index(drop=True)
    
    return data

def get_product_recommendation(
    product_recommendation: pd.DataFrame,
    detail_mitra: pd.DataFrame,
    gcs: GoogleCloudStorage
) -> pd.DataFrame:
    """
    Obtain data regarding to product recommendation, which based from context enrichment data

    Parameters
    ----------
        product_recommendation: pd.DataFrame
            raw data about product recommendation from context enrichment

        detail_mitra: pd.DataFrame
            data regarding to detail mitra

        gcs: GoogleCloudStorage
            an instance of Google Cloud Storage

    Returns
    ----------
        product_recommendation: pd.DataFrame
            structured product recommendation data
    """
    # Data Preprocessing
    product_recommendation: pd.DataFrame = modify_data(product_recommendation)
    product_recommendation.drop("snapshot_dt", axis=1, inplace=True, errors="ignore")
    product_recommendation["nama_produk"] = product_recommendation["nama_produk"].apply(lambda product: product.upper().strip()).replace({"TIDAK ADA": "Tidak ada"})

    # Get SMRM and GMV data
    smrm_data: pd.DataFrame = get_smrm_data(gcs)
    gmv_data: pd.DataFrame = get_gmv_data(gcs)

    # Concatenate product recommendation with mitra details
    product_recommendation = product_recommendation.merge(
        detail_mitra,
        on="mitra_id"
    ).rename(columns={"region_mitra": "region"})

    # Concatenate product recommendation with GMV data
    product_recommendation = product_recommendation.merge(
        gmv_data,
        how="left",
        on=["mitra_id", "nama_produk"]
    )

    # Concatenate product recommendation with SMRM data
    product_recommendation = product_recommendation.merge(
        smrm_data,
        how="left",
        on=["region", "nama_produk"]
    )

    # Choose products with positive smrm rate only
    product_recommendation = product_recommendation[product_recommendation["smrm_rate"] > 0]

    # Obtain first five products with highest SMRM for each mitra
    product_recommendation = product_recommendation.sort_values(["mitra_id", "total_gmv", "smrm_rate"], ascending=[True, False, False]).groupby("mitra_id").head(5)
    return product_recommendation.drop(["total_gmv", "smrm_rate"], axis=1, errors="ignore")
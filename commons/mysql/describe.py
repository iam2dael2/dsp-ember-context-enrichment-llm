import numpy as np
import pandas as pd
from mysql.connector.cursor_cext import MySQLCursorAbstract
from typing import List

def describe_table(
    cursor: MySQLCursorAbstract,
    table_name: str
) -> pd.DataFrame:
    """
    Show field's description related to the table

    Parameters
    ----------
        cursor: MySQLCursorAbstract
            Abstract cursor class

        table_name: str
            Name of table
    
    Returns
    ----------
        table_desc: pd.DataFrame
            Description of table's fields
    """
    # Check the modified table
    cursor.execute(f"desc {table_name}") 

    # Fetch the result
    result = cursor.fetchall() 

    # Specify columns and rows of end result
    columns: List[str] = ["Field", "Type", "Null", "Key", "Default", "Extra"]
    values: List[str] = np.array(result).T.tolist()

    table_desc: pd.DataFrame = pd.DataFrame(data=dict(zip(columns, values)))
    return table_desc
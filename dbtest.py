import pyodbc
from tabulate import tabulate
from time import sleep
import pandas as pd
from datetime import datetime, timedelta
from typing import *

THRESHOLD_SECONDS = 24 * 3600

def init_db() -> pyodbc.Connection:  
    server = r"secret"
    database = "secret"
    username = "secret"
    password = "secret"
    driver = "{ODBC Driver 17 for SQL Server}"

    connection_string = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    
    return pyodbc.connect(connection_string)

def get_printed_lots() -> List[str]:
    printed_lots = list()
    
    cnxn = init_db()
    
    strSQL = """
        SELECT
            p.DSPGLotNumber,
            p.SetupDate,
            p.PrintDate,
            lo.Layout,
            lo.Layer
        FROM [secret].[dbo].[Printing] p 
        INNER JOIN [secret].[dbo].[Inventory] l
            ON p.InventoryId = l.ID
        INNER JOIN [secret].[dbo].[Layout] lo
            ON l.LayoutId = lo.ID
    """

    data = pd.read_sql_query(strSQL, cnxn)

    return data


def get_incomplete_lots(data: pd.DataFrame) -> pd.DataFrame:
    # Current time
    now = datetime.now()
    
    # Filter rows where PrintDate is missing and the SetupDate is younger than the threshold
    incomplete_lots = data[
        (data['PrintDate'].isna()) & 
        ((now - pd.to_datetime(data['SetupDate'])).dt.total_seconds() < THRESHOLD_SECONDS)
    ]
    
    return incomplete_lots[['DSPGLotNumber', 'SetupDate', 'PrintDate', 'Layout', 'Layer']]

print(tabulate(get_printed_lots()))
print(tabulate(get_incomplete_lots(get_printed_lots())))
# Hartsell
# 25 Apr 2024

# Imports
import os
import re
import shutil
from datetime import datetime, timedelta
from time import sleep
from typing import *
import warnings

import pyodbc
import pandas as pd

# Globals

LOCAL_FOLDER = r"D:\BatchLogs"
# LOCAL_FOLDER = r".\data" # dev folder
SERVER_FOLDER = r"\\10.225.43.45\prod-critical\LTCC\DSP\DSP_Print\BatchLogs"
SM_INI_PATH = r"\\10.225.43.45\prod-critical\LTCC\DSP\DSP_Print\dsp_print_sdd.ini"
TD_HRS = 1  # exclude all files created within x hours
THRESHOLD_SECONDS = 24 * 3600 # if no print date, exclude until over x sec old

# Methods
def init_db() -> pyodbc.Connection:  
    server = r"secret"
    database = "secret"
    username = "dspuser"
    password = "dspus3r"
    driver = "{ODBC Driver 17 for SQL Server}"

    connection_string = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    
    return pyodbc.connect(connection_string)


def get_all_filenames(
    dirname,
) -> Generator[str, None, None]:  # return iterator for all filenames in directory
    with os.scandir(dirname) as dir_entries:
        for entry in dir_entries:
            if entry.is_dir():
                yield from get_all_filenames(entry.path)
            elif entry.is_file():
                yield entry.path


def get_running_lots() -> List[str]:
    running_lots = list()
    with open(SM_INI_PATH, "r") as f:
        content = f.read()

    lot_pattern = r"Lot=(\d+)"
    lot_prog = re.compile(lot_pattern)
    for line in content.splitlines():
        match = lot_prog.search(line)
        if match:
            running_lots.append(str(match[1]))
            # print(f"Currently running lot {match[1]}")
    return running_lots

def get_printed_lots() -> List[str]:    
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

    warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
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

# Main
def main():
    # Get local and server files
    print("Getting file lists...")
    local_files = list(get_all_filenames(LOCAL_FOLDER))
    server_files = list(get_all_filenames(SERVER_FOLDER))

    print("Local files:")
    for f in local_files: print(f)
    print("Server files:")
    for f in server_files: print(f)

    # remove files already on server
    local_files = [f for f in local_files if f not in server_files]

    # Remove currently running files
    running_lots = get_running_lots()
    local_files_to_remove = [f for f in local_files if any(lot in os.path.basename(f) for lot in running_lots)]
    local_files = [f for f in local_files if f not in local_files_to_remove]

    # Get incomplete lots according to screen management records
    incomplete_lots = get_incomplete_lots(get_printed_lots())['DSPGLotNumber'].astype(str).tolist()
    incomplete_lots_without_plantcode = [lot[-6:] for lot in incomplete_lots]

    # Filter out files associated with incomplete lots
    local_files = [f for f in local_files if not any(suffix in f for suffix in incomplete_lots_without_plantcode)]

    # remove new(ish) files - defined in TD_HRS
    local_files = [
        f for f in local_files
        if datetime.fromtimestamp(os.stat(f).st_ctime) < datetime.now() - timedelta(hours=TD_HRS)
    ]

    print("Files to move:")
    for f in local_files: print(f)

    print("Moving files...")
    for filepath in local_files:
        filename = os.path.basename(filepath)
        parent_folder = os.path.basename(os.path.dirname(filepath))
        new_path = os.path.join(SERVER_FOLDER, parent_folder, filename)

        if not os.path.exists(os.path.join(SERVER_FOLDER, parent_folder)):
            os.makedirs(os.path.join(SERVER_FOLDER, parent_folder))
        shutil.copy(filepath, new_path)
        print(f"copied {filename}")

    # verify files moved correctly
    print("Verifying...")
    server_files = get_all_filenames(SERVER_FOLDER)
    if any(f not in server_files for f in local_files):
        print("Failed to move some files!")
    else:
        print("Success!")


# Run
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(repr(e))
    finally:
        print("Program finished, exiting...")
        sleep(1)

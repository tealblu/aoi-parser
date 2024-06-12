# Hartsell
# 24 April 2024

# Configuration variables
DEV = False  # developer mode toggle
OUT_FILE = r".\parsing-log.txt"  # log file path

# Imports

import os
import re
from datetime import datetime
from typing import *

import pyodbc

# Globals
DATE_FORMAT = "%m/%d/%Y"
SM_INI_PATH = r"\\10.225.43.45\prod-critical\LTCC\DSP\DSP_Print\dsp_print_sdd.ini"
DATA_PATH = (
    r"\\10.225.43.45\prod-critical\LTCC\DSP\DSP_Print\BatchLogs"  # data folder path
)
cursor = pyodbc.Cursor
existing_lot_data_keys = set()  # global set of lots that we have already

# precompile regular expressions
layer_prog = re.compile(r"_([A-Z]\d+)_")
prog = re.compile(r"\[(.*?)\](.*?)(?=\n\[|$)")
circuit_prog = re.compile(
    r"ES\s+(\d+)\s+FC(?:\s+(\d+))?\s+Length\s+(\d+\.\d+)\s+Breadth\s+(\d+\.\d+)\s+Area\s+(\d+\.\d+)"
)
lot_prog = re.compile(r"Lot=(\d+)")


# Classes
class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    ENDC = "\033[0m"

    # Method that returns a message with the desired color
    # usage:
    #    print(bcolor.colored("My colored message", bcolor.OKBLUE))
    @staticmethod
    def colored(message, color):
        return color + message + bcolors.ENDC

    # Method that returns a yellow warning
    # usage:
    #   print(bcolors.warning("What you are about to do is potentially dangerous. Continue?"))
    @staticmethod
    def warning(message):
        return bcolors.WARNING + message + bcolors.ENDC

    # Method that returns a red fail
    # usage:
    #   print(bcolors.fail("What you did just failed massively. Bummer"))
    #   or:
    #   sys.exit(bcolors.fail("Not a valid date"))
    @staticmethod
    def fail(message):
        return bcolors.FAIL + message + bcolors.ENDC

    # Method that returns a green ok
    # usage:
    #   print(bcolors.ok("What you did just ok-ed massively. Yay!"))
    @staticmethod
    def ok(message):
        return bcolors.OKGREEN + message + bcolors.ENDC

    # Method that returns a blue ok
    # usage:
    #   print(bcolors.okblue("What you did just ok-ed into the blue. Wow!"))
    @staticmethod
    def okblue(message):
        return bcolors.OKBLUE + message + bcolors.ENDC

    # Method that returns a header in some purple-ish color
    # usage:
    #   print(bcolors.header("This is great"))
    @staticmethod
    def header(message):
        return bcolors.HEADER + message + bcolors.ENDC


# class to hold lot data
class LotData:
    def __init__(self):
        self.lotNum: int = 0
        self.machine: str = "NULL"
        self.layout: str = "NA"
        self.layer: str = "NA"
        self.startDate: datetime = None
        self.endDate: datetime = None

        self.substrateCnt: int = 0

        self.inputES: int = 0
        self.reviewedES: int = 0
        self.goodES: int = 0
        self.rejectES: int = 0
        self.outputES: int = 0

        self.circuitData: List[CircuitData] = list()

    def __repr__(self) -> str:
        reprStr = ""
        reprStr += f"lotNum: {self.lotNum}\n"
        reprStr += f"machine: {self.machine}\n"
        reprStr += f"layout: {self.layout}\n"
        reprStr += f"layer: {self.layer}\n"
        reprStr += f"num substrates: {self.substrateCnt}\n"
        reprStr += f"num failures: {len(self.circuitData)}\n"

        return reprStr


# class to hold circuit data
class CircuitData:
    def __init__(self):
        self.lotNum: int = 0
        self.substrateNum: int = 0
        self.circuitNum: int = 0
        self.status: str = "No Failure"
        self.didStop: bool = False
        self.length: int = 0
        self.breadth: int = 0
        self.area: int = 0

    def __repr__(self) -> str:
        reprStr = ""
        reprStr += f"lotNum: {self.lotNum}\n"
        reprStr += f"substrateNum: {self.substrateNum}\n"
        reprStr += f"circuitNum: {self.circuitNum}\n"
        reprStr += f"status: {self.status}\n"
        reprStr += f"dimensions: {self.length} x {self.breadth}\n"
        reprStr += f"area: {self.area}\n"

        return reprStr


# Functions
def log(msg: str):  # simple logger
    if DEV:
        print(f"{datetime.now()}: {msg}")
        with open(OUT_FILE, "a") as of:
            print(f"{datetime.now()}: {msg}", file=of)
    else:
        print(f"{datetime.now()}: {msg}")


def init_db() -> pyodbc.Connection:  # connect to db
    # connection parameters
    server = r"secret"
    database = "LTCC_PRO"
    username = "dspuser"
    password = "dspus3r"
    driver = "{ODBC Driver 17 for SQL Server}"

    # connect to database
    cnxn = pyodbc.connect(
        "DRIVER="
        + driver
        + ";SERVER="
        + server
        + ";DATABASE="
        + database
        + ";UID="
        + username
        + ";PWD="
        + password
    )
    return cnxn


# sort list[lotData] by start date
def sort_by_startDate(lot_data_list: List[LotData]) -> List[LotData]:
    return sorted(lot_data_list, key=lambda lot: lot.startDate)


# extract time from string
def extract_time(time_str: str) -> List[int]:
    # Split the time string into hours, minutes, and AM/PM
    parts = time_str.split(":")
    hour_str, minute_str = parts[0], parts[1][:2]
    am_pm = parts[1][2:].strip().upper()

    # Convert hour to 24-hour format
    hour = int(hour_str)
    if am_pm == "PM" and hour != 12:
        hour += 12
    elif am_pm == "AM" and hour == 12:
        hour = 0

    # Extract minute and second
    minute = int(minute_str)
    second = 0

    return hour, minute, second


# main parsing function
# loops thru file line by line and extracts data
def parse_data_from_file(path: str) -> LotData:
    # Ensure path exists
    if not os.path.isfile(path):
        return Exception(f"{path} is not a file!")
    else:
        log(f"Parsing data from {path}...")
        with open(path, "r") as f:
            content = f.read()

    # Initialize
    data = LotData()
    substrateNum = 0
    bak_substrateCnt = 0

    # extract layer from filename
    layer_match = layer_prog.search(os.path.basename(path))
    if layer_match:
        data.layer = layer_match[0][1:3]  # get 'A2' from '_A2_'
    else:
        data.layer = "NA"

    for line in content.splitlines():
        match = prog.search(line)
        if match:
            param = match[1].strip()
            value = match[2].strip()

            if param == "Machine":
                if value == "BoschDsp - AOI":
                    data.machine = "NULL"
                else:
                    data.machine = value
            elif param == "Typ":
                data.layout = value
            elif param == "ChargenNr":
                data.lotNum = value
                # Check for sister lots
                if "-" in data.lotNum:
                    oldLotNum = data.lotNum
                    num, sister = data.lotNum.split("-")
                    data.lotNum = int(str(num) + str(sister))
                    log(
                        bcolors.warning(
                            f"Amending sister lot {oldLotNum} -> {data.lotNum}"
                        )
                    )
            elif param == "StartDate":
                data.startDate = datetime.strptime(value, DATE_FORMAT)
            elif param == "StartTime":
                hour, minute, second = extract_time(value)
                data.startDate = data.startDate.replace(
                    hour=hour, minute=minute, second=second
                )
            elif param == "EndDate":
                data.endDate = datetime.strptime(value, DATE_FORMAT)
            elif param == "EndTime":
                hour, minute, second = extract_time(value)
                data.endDate = data.endDate.replace(
                    hour=hour, minute=minute, second=second
                )
            elif param == "GS-Input":
                data.substrateCnt = int(value)
            elif param == "ES-Input":
                data.inputES = value
            elif param == "ES-Reviewed":
                data.reviewedES = value
            elif param == "ES-Good":
                data.goodES = value.split()[0]
            elif param == "Total-rejects":
                data.rejectES = value
            elif param == "ES-Output":
                data.outputES = value
            elif param == "GS":
                substrateNum = value.split("A")[0]
                bak_substrateCnt += 1
        elif line.startswith("\tES "):  # extract circuit data
            circuit_data = CircuitData()

            circuit_data.lotNum = data.lotNum
            circuit_data.substrateNum = substrateNum  # starts at 1!

            circuit_match = circuit_prog.search(line)
            if circuit_match:
                circuit_data.circuitNum = circuit_match[1]

                if circuit_match[2] == "1001":
                    circuit_data.status = "NonRepairable"
                elif circuit_match[2] == "1002":
                    circuit_data.status = "Repairable"
                elif circuit_match[2] == "1003":
                    circuit_data.status = "FalseDefect"
                else:
                    circuit_data.status = "NotReviewed"

                circuit_data.length = circuit_match[3]
                circuit_data.breadth = circuit_match[4]
                circuit_data.area = circuit_match[5]
            else:  # corrupted data on this line
                circuit_data.circuitNum = -1
                circuit_data.status = "Unknown"
                circuit_data.length = -1
                circuit_data.breadth = -1
                circuit_data.area = -1

            # Check if circuit caused stop
            if "Serial" in line and "True" in line:
                circuit_data.didStop = True

            data.circuitData.append(circuit_data)

            log("Circuit data extracted:" + "\n" + repr(circuit_data))
        elif line.startswith("\tNo Failure"):
            # continue  # don't do anything to lines that don't have a failure - reduce lines in DB
            circuit_data = CircuitData()
            circuit_data.lotNum = data.lotNum
            circuit_data.substrateNum = substrateNum
            circuit_data.circuitNum = 0
            circuit_data.status = "No Failure"

            log("Circuit data extracted:" + "\n" + repr(circuit_data))
            # data.circuitData.append(circuit_data)
            continue

    log("Lot data extracted:" + "\n" + repr(data))

    return (
        data
        if (data.lotNum, data.machine, data.layer) not in existing_lot_data_keys
        else Exception(f"Lot {data.lotNum} has already been parsed!")
    )


# get currently running lots so we don't parse them
def get_running_lots() -> List[str]:
    running_lots = list()
    with open(SM_INI_PATH, "r") as f:
        content = f.read()

    for line in content.splitlines():
        match = lot_prog.search(line)
        if match:
            running_lots.append(str(match[1]))
            log(f"Currently running lot {match[1]}")
    return running_lots


# get list of lots that we have already parsed
def get_parsed_lots() -> List[Tuple[str, str]]:
    parsed_lots = list()

    # get all lotnums from SQL
    strSQL = "SELECT lotNum, layer from LTCC_PRO.dspg.lot_data"
    cursor.execute(strSQL)

    rows = cursor.fetchall()
    for row in rows:
        parsed_lots.append((row[0], row[1]))

    return parsed_lots


# Main
def main():
    print(bcolors.header("Program started."))
    print("Please wait...")

    # Setup
    log("Initializing...")
    cnxn = init_db()
    global cursor
    cursor = cnxn.cursor()

    lots_to_skip = list()

    log("Getting lots to skip...")
    # get old lots from SQL
    lot_layers: Dict[str, List[str]] = {}
    for lot, layer in get_parsed_lots():
        lot = str(lot)

        # Check for sister lots
        if "-" in str(lot):
            oldLotNum = lot
            num, sister = lot.split("-")
            lot = str(num) + str(sister)
            log(bcolors.warning(f"Amending sister lot {oldLotNum} -> {lot}"))

        lots_to_skip.append(lot)

        if lot in lot_layers.keys():
            lot_layers[lot].append(layer)
        else:
            lot_layers[lot]: List[str] = list()
            lot_layers[lot].append(layer)

    # skip currently running lot and its sister lot
    current_lots = list()
    for lot in get_running_lots():
        # Check for sister lots
        if "-" in str(lot):
            oldLotNum = lot
            num, sister = lot.split("-")
            lot = int(str(num) + str(sister))
            log(bcolors.warning(f"Amending sister lot {oldLotNum} -> {lot}"))

        current_lots.append(lot)
        current_lots.append(str(int(lot) + 1))

    # Fetch existing primary key combinations from database
    log("Fetching existing primary key combinations from database...")
    global existing_lot_data_keys
    strSQL = "SELECT lotNum, machine, layer FROM LTCC_PRO.dspg.lot_data"
    cursor.execute(strSQL)
    for row in cursor.fetchall():
        lot = row.lotNum
        # Check for sister lots
        if "-" in str(lot):
            oldLotNum = lot
            num, sister = lot.split("-")
            lot = int(str(num) + str(sister))
            log(bcolors.warning(f"Amending sister lot {oldLotNum} -> {lot}"))

        existing_lot_data_keys.add((lot, row.machine, row.layer))

    existing_circuit_data_keys = set()
    strSQL = (
        "SELECT lotNum, substrateNum, circuitNum, layer FROM LTCC_PRO.dspg.circuit_data"
    )
    cursor.execute(strSQL)
    for row in cursor.fetchall():
        lot = str(row.lotNum)
        if "-" in str(lot):
            oldLotNum = lot
            num, sister = lot.split("-")
            lot = str(num) + str(sister)
            log(bcolors.warning(f"Amending sister lot {oldLotNum} -> {lot}"))

        existing_circuit_data_keys.add(
            (lot, row.substrateNum, row.circuitNum, row.layer)
        )

    # walk thru DATA_PATH and get data for each lot
    log("Parsing new log files...")
    all_lots_data = list()
    for dirpath, dirnames, filenames in os.walk(DATA_PATH):
        for file in filenames:
            # Check if lot-layer pair has been parsed already, if so, skip
            if any(
                str(lotNum)[:6] in file and layer in file
                for lotNum, machine, layer in existing_lot_data_keys
            ) or any(str(lotNum) in file for lotNum in current_lots):
                continue
            else:
                fp = os.path.join(dirpath, file)

                try:
                    lot_data = parse_data_from_file(fp)
                except Exception as e:
                    log(bcolors.warning(f"Error while parsing: {repr(e)}"))
                    continue

                all_lots_data.append(lot_data)

    # Sort all_lots_data
    all_lots_data = sort_by_startDate(all_lots_data)

    # Remove existing lots
    all_lots_data = list(
        (
            lot
            for lot in all_lots_data
            if (lot.lotNum, lot.machine, lot.layer) not in existing_lot_data_keys
        )
    )

    if len(all_lots_data) > 0:
        log(bcolors.okblue(f"{len(all_lots_data)} lots to upload:"))
    else:
        log("No new lots found.")

    for lot in all_lots_data:
        log(f"Uploading lot {lot.lotNum}, layer {lot.layer} to SQL...")
        lot_key = (lot.lotNum, lot.machine, lot.layer)
        existing_lot_data_keys.add(lot_key)

        dbTable = "LTCC_PRO.dspg.lot_data"
        strSQL = f"INSERT INTO {dbTable} (lotNum, machine, layout, startDate, endDate, inputES, reviewedES, goodES, rejectES, outputES, layer, substrateCnt) VALUES ({lot.lotNum}, '{lot.machine}', '{lot.layout}', '{lot.startDate}', '{lot.endDate}', {lot.inputES}, {lot.reviewedES}, {lot.goodES}, {lot.rejectES}, {lot.outputES}, '{lot.layer}', {lot.substrateCnt})"
        # log(strSQL)

        try:
            cursor.execute(strSQL)
        except:
            print("Error executing query!")

        if cursor.rowcount != 1:
            log(bcolors.warning("SQL ERROR!"))

        for circuit in lot.circuitData:
            circuit.lotNum = lot.lotNum
            circuit_key = (
                circuit.lotNum,
                circuit.substrateNum,
                circuit.circuitNum,
                lot.layer,
            )
            if (
                circuit_key in existing_circuit_data_keys
            ):  # Have we already processed the circuit?
                # Handle circuit already being in DB
                log(
                    bcolors.warning(
                        f"Circuit {circuit_key} already exists in database. Checking status..."
                    )
                )

                strSQL = f"SELECT status from LTCC_PRO.dspg.circuit_data WHERE lotNum = {circuit.lotNum} AND substrateNum = {circuit.substrateNum} AND circuitNum = {circuit.circuitNum} AND layer = '{lot.layer}'"
                cursor.execute(strSQL)
                row = cursor.fetchone()
                if row:
                    if (
                        row.status != "NonRepairable"
                        and circuit.status == "NonRepairable"
                    ):  # circuit is not repairable, remove and update db record
                        strSQL = f"DELETE FROM LTCC_PRO.dspg.circuit_data WHERE lotNum = {circuit.lotNum} AND substrateNum = {circuit.substrateNum} AND circuitNum = {circuit.circuitNum} AND layer = '{lot.layer}'"
                        cursor.execute(strSQL)
                        log(bcolors.okblue(f"Updating {circuit_key}..."))
                    else:  # circuit is NOT not repairable, skip it
                        continue
            else:
                existing_circuit_data_keys.add(circuit_key)

            dbTable = "LTCC_PRO.dspg.circuit_data"
            stopBit = 1 if circuit.didStop is True else 0
            strSQL = f"INSERT INTO {dbTable} (lotNum, substrateNum, circuitNum, status, length, breadth, area, didStop, layer) VALUES ({lot.lotNum}, {circuit.substrateNum}, {circuit.circuitNum}, '{circuit.status}', {circuit.length}, {circuit.breadth}, {circuit.area}, {stopBit}, '{lot.layer}')"
            # log(strSQL)

            try:
                cursor.execute(strSQL)
            except:
                print("Error executing query!")

            if cursor.rowcount != 1:
                log(bcolors.warning("SQL ERROR!"))

    try:
        # Commit all changes
        cnxn.commit()
    except Exception as e:
        log(bcolors.warning("Error with SQL Transaction!"))
        log(bcolors.fail(repr(e)))
    finally:
        cnxn.close()


if __name__ == "__main__":
    try:
        main()
    finally:
        log("Program finished!")

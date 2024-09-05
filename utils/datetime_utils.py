import datetime
import os

def build_data_storage_path(base_path, date):
    """
    Build the path to store the GH Archive data for the specified date.
    
    :param base_path: Base path where the data will be stored.
    :param date: Date in YYYY-MM-DD format.
    :return: Path to store the data.
    """
    year, month, day = date.year, date.month, date.day
    return os.path.join(base_path, f"year={str(year)}", f"month={str(month)}", f"day={str(day)}")


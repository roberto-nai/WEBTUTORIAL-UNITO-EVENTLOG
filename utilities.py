# utilities.py
import pandas as pd

def df_read_csv_data(path_csv: str, col_list: list, csv_sep: str = ",") -> pd.DataFrame:
    """
    Reads data from a CSV file into a pandas DataFrame with specified columns and data types.

    Parameters:
        path_csv (str): the file path to the CSV file to be read.
        col_list (list): a list of column names to be extracted.
        sep (str): the delimiter string used in the CSV file. Defaults to ';'.

    Returns:
        pd.DataFrame: a pandas DataFrame containing the data read from the CSV file.
    """
    df = None
    if col_list is None:
        df = pd.read_csv(path_csv, sep=csv_sep, low_memory=False)
    else:
        df = pd.read_csv(path_csv, sep=csv_sep, usecols=col_list, low_memory=False)
    df = df.drop_duplicates()

    print("Data preview")
    print(df.head())
    print()
    print("Shape:", df.shape)
    print("Rows:", len(df))
    print("Columns:", df.columns)
    print()

    return df

def df_show_data(df: pd.DataFrame) -> None:
    print("Data preview")
    print(df.head())
    print()
    print("Shape:", df.shape)
    print("Rows:", len(df))
    print("Columns:", df.columns)
    print()

def df_get_unique_values(df: pd.DataFrame, col_list: list) -> dict:
    """
    Obtain unique values for each specified column in a DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame from which to extract unique values.
        col_list (list): List of column names to check for unique values.

    Returns:
        dict: A dictionary where keys are column names and values are arrays of unique values.
    """
    unique_values = {}

    for col in col_list:
        unique_values[col] = df[col].unique()

    return unique_values

def dict_with_formatting(data: dict) -> None:
    """
    Print a dictionary with each key on a new line and the corresponding values on the line below, with a blank line between each key-value pair.

    Parameters:
        data (dict): The dictionary to be printed.
    
    Returns:
        None
    """
    dict_size = len(data)
    print(f"Dictionary size: {dict_size}\n")
    i = 0
    for key, value in data.items():
        i+=1
        print(f"[{i}] Key: {key}")
        print(f" - Values: {value}")
        print()

def df_retain_columns(df: pd.DataFrame, columns_to_keep:list):
    """
    Retains only the specified columns in the DataFrame.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        columns_to_keep (list): A list of column names to retain in the DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing only the specified columns.
    """
    
    # Check if the DataFrame contains the specified columns
    missing_columns = [col for col in columns_to_keep if col not in df.columns]
    if missing_columns:
        print(f"The DataFrame is missing the following columns: {', '.join(missing_columns)}")
        # Optionally, you can decide to return an empty DataFrame or a DataFrame with available columns
        return df[columns_to_keep if all(col in df.columns for col in columns_to_keep) else [col for col in columns_to_keep if col in df.columns]]

    # Retain only the specified columns
    filtered_df = df[columns_to_keep]

    return filtered_df

def df_rename_columns(df: pd.DataFrame, rename_dict:dict) -> pd.DataFrame:
    """
    Renames columns 'event' to 'eventPage' and 'lastUpdate' to 'eventTimestamp'.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        rename_dict (dict): Dictionary with renaming mapping.
    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """
    
    # Check if the columns to be renamed exist in the DataFrame
    missing_columns = [col for col in rename_dict.keys() if col not in df.columns]
    if missing_columns:
        print(f"The DataFrame is missing the following columns to rename: {', '.join(missing_columns)}")
        # Optionally, continue with available columns
        rename_dict = {k: v for k, v in rename_dict.items() if k in df.columns}

    # Rename the columns
    df = df.rename(columns=rename_dict)

    return df


def df_count_distinct_id(df: pd.DataFrame, keyword:str, key_column:str) -> int:
    """
    Counts distinct sessionIDs in the DataFrame where the pageTitle column contains the specified keyword.

    Parameters:
        df (pd.DataFrame): The DataFrame to be processed.
        keyword (str): The keyword to filter by in the pageTitle column.
        key_column (str): The key column to find distinct values.
    Returns:
        int: The count of unique key_column.
    """
    # Filter rows where 'pageTitle' contains the specified keyword
    filtered_pages = df[df['pageTitle'].str.contains(keyword, case=False, na=False)]

    # Remove duplicate sessionIDs
    unique_sessions = filtered_pages[key_column].drop_duplicates()

    # Count the unique sessionIDs
    distinct_session_count = unique_sessions.count()

    return distinct_session_count
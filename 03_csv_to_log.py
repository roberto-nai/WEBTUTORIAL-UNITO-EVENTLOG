"""
03_csv_to_log.py
"""

### IMPORT ###
from pathlib import Path
from datetime import datetime, time
import pandas as pd
import matplotlib.pyplot as plt


### LOCAL IMPORT ###
from config import config_reader
from utilities import df_read_csv_data, df_get_unique_values, df_show_data, df_retain_columns, df_rename_columns, dict_with_formatting, df_remove_rows_with_substring 

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
# print(yaml_config) # debug
data_dir = str(yaml_config["DATA_DIR"])
log_dir = str(yaml_config["LOG_DIR"])
stats_dir = str(yaml_config["STATS_DIR"])
plots_dir = str(yaml_config["PLOTS_DIR"])

events_file = str(yaml_config["EVENTS_FILE"]) # input
quiz_stats_file = str(yaml_config["QUIZ_STATS_FILE"]) # input
survey_file_clean = str(yaml_config["SURVEY_GOOGLE_FILE_CLEAN"]) # input
sus_file = str(yaml_config["SUS_FILE"]) # input
case_len_threshold = int(yaml_config["CASE_LEN_THRESHOLD"])
case_time_threshold = int(yaml_config["CASE_TIME_THRESHOLD"])
disco_cases = str(yaml_config["DISCO_CASES_FILE"]) # input
id_column = "Case ID" # Final trace identifier
activity_column = "Activity"
timestamp_column = "Complete Timestamp"

# Filter data based on list of cases already filtered in DISCO
filter_disco_cases = 1 # 1 = yes, 0 = no

# Dictionary of pageTitle ITA to ENU
dic_en_pageTitle = {'Introduzione':'INTRO', 'Introduzione-Quiz':'INTRO-Q', 'Primo programma':'PROG', 
                    'Primo programma-Quiz': 'PROG-Q', 'Variabili':'VARS', 'Variabili-Quiz':'VARS-Q',
                    'Istruzione if':'IF_ELSE', 'Istruzione if-Quiz':'IF_ELSE-Q', 'Ciclo for':'FOR',
                    'Ciclo for-Quiz':'FOR-Q', 'Tipi di dato':'TYPES', 'Tipi di dato-Quiz':'TYPES-Q',
                    'Conversioni':'CONV', 'Conversioni-Quiz':'CONV-Q', 'Liste':'LISTS', 'Liste-Quiz':'LISTS-Q',
                    'Dizionari':'DICTS', 'Dizionari-Quiz':'DICTS-Q', 'Funzioni':'FUNCT', 
                    'Funzioni-Quiz':'FUNCT-Q', 'Survey':'SURVEY-START'}

# Dictionary of event ITA to ENU
dic_en_event = {'ingressoPagina':'PageIN', 'mouseover':'MouseIN', 'mouseout':'MouseOUT', 'mouseenter':'MouseENT',
                'uscitaPagina':'PageOUT', 'click':'CLICK', 'dbclick':'DBCLICK'}

clik_event_list = ['CLICK', 'DBCLICK'] # Frequency events per sessionID

# Criteria "Class" structure
criteria = [
    {'date': datetime(2024, 3, 7).date(), 'start_time': time(0, 0), 'end_time': time(23, 59), 'class': 'SAA'},
    {'date': datetime(2024, 3, 19).date(), 'start_time': time(0, 0), 'end_time': time(23, 59), 'class': 'ECO'},
    {'date': datetime(2024, 4, 18).date(), 'start_time': time(10, 45), 'end_time': time(12, 59), 'class': 'SMTO1'},
    {'date': datetime(2024, 4, 18).date(), 'start_time': time(13, 0), 'end_time': time(15, 14), 'class': 'SMTO2'},
    {'date': datetime(2024, 4, 18).date(), 'start_time': time(15, 15), 'end_time': time(23, 59), 'class': 'SMTO3'},
    {'date': datetime(2024, 4, 22).date(), 'start_time': time(11, 45), 'end_time': time(13, 59), 'class': 'SMCN1'},
    {'date': datetime(2024, 4, 22).date(), 'start_time': time(14, 0), 'end_time': time(23, 59), 'class': 'SMCN2'}
]
"""
Data;Ora;Classe
2024-03-07;SAA
2024-03-19;ECO
2024-04-18;dalle 10:45 alle 12.59;SMTO1
2024-04-18;dalle 13:00 alle 15:14;SMTO2
2024-04-18;dalle 15:15 in avanti;SMTO3
2024-04-22;dalle 11:45 alle 13:59;SMCN1
2024-04-22;dalle 14:00 in avanti;SMCN2
"""

### FUNCTIONS ###
def replace_page_titles(df: pd.DataFrame, mapping_dict: dict) -> pd.DataFrame:
    """
    Replaces the values in the 'pageTitle' column of the dataframe df with the values defined in the mapping_dict dictionary.
    
    Parameters:
        df (pd.DataFrame): The dataframe containing the 'pageTitle' column.
        mapping_dict (dict): The dictionary with the mapping of values.
    
    Returns:
        pd.DataFrame: The dataframe with the 'pageTitle' column values replaced.
    """
    df['pageTitle'] = df['pageTitle'].replace(mapping_dict)
    return df

def add_event_para_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column 'eventPara' to the DataFrame by concatenating 'pageTitle', 'event', and 'pagePara'.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing 'pageTitle', 'event', and 'pagePara' columns.

    Returns:
        pd.DataFrame: The DataFrame with the added 'eventPara' column.
    """
    
    # Check that the DataFrame contains the required columns
    required_columns = ['pageTitle', 'pagePara', 'event']
    for column in required_columns:
        if column not in df.columns:
            print(f"The DataFrame is missing the required column: {column}")
            return df

    # Format 'pagePara' to add a leading zero if less than 10
    # df['formattedPagePara'] = df['pagePara'].apply(lambda x: f'{x:02}')

    # Concatenate the strings in the desired format
    # df['eventPara'] = df['pageTitle'] + '_' + df['event'] + '_' + df['formattedPagePara']
    df['eventPara'] = df['pageTitle'] + '_' + df['event'] + '_' + df['pagePara'].astype(str)

    # Remove the temporary 'formattedPagePara' column
    # df.drop(columns=['formattedPagePara'], inplace=True)

    return df

def find_and_fix_ts_duplicates(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Adds 1 second to equal timestamps for sessionID.
    Instead of sorting on the initial three columns, sort the DataFrame by "sessionID" and "eventTimestamp". This ensures that any duplicates are adjacent to each other. 

    Parameters:
        df_input (pd.DataFrame): The dataframe containing the data.

    Returns:
        pd.DataFrame: A dataframe with eventTimestamp fixed (if needed).
    """
    
    df_input_len = len(df_input)

    df_sorted = df_input.sort_values(by=['sessionID', 'eventTimestamp']).reset_index(drop=True)

    count_duplicates = 0

    # Identify and modify duplicates
    for i in range(1, len(df_input)):
        # If the current row has the same 'sessionID' and 'eventTimestamp' as the previous row, modify the 'eventTimestamp'
        # of the current row (i) to be one second later than the row before it (i-1)
        if (df_sorted.iloc[i]['sessionID'] == df_sorted.iloc[i - 1]['sessionID']) and (df_sorted.iloc[i]['eventTimestamp'] == df_sorted.iloc[i - 1]['eventTimestamp']):
            # Increment the 'eventTimestamp' by 1 second from the previous row
            count_duplicates += 1
            # print(f"Duplicated found at sessionID {df_sorted.iloc[i]['sessionID']}") # debug
            # print("Old value (duplicate):", df_sorted.iloc[i]['eventTimestamp'], "=", df_sorted.iloc[i-1]['eventTimestamp']) # debug
            df_sorted.iloc[i, df_sorted.columns.get_loc('eventTimestamp')] += pd.Timedelta(seconds=1)
            # print("New value for row:", i, ":", df_sorted.iloc[i, df_sorted.columns.get_loc('eventTimestamp')]) # debug

    print(f"Duplicates corrected: {count_duplicates} / {df_input_len}")
    print()
    return df_sorted

def count_distinct_sessions_by_title(df: pd.DataFrame, keyword:str) -> int:
    """
    Counts distinct sessionIDs in the DataFrame where the pageTitle column contains the specified keyword.

    Parameters:
        df (pd.DataFrame): The DataFrame to be processed.
        keyword (str): The keyword to filter by in the pageTitle column.

    Returns:
        int: The count of unique sessionIDs.
    """
    # Filter rows where 'pageTitle' contains the specified keyword
    filtered_pages = df[df['pageTitle'].str.contains(keyword, case=False, na=False)]

    # Remove duplicate sessionIDs
    unique_sessions = filtered_pages['sessionID'].drop_duplicates()

    # Count the unique sessionIDs
    distinct_session_count = unique_sessions.count()

    return distinct_session_count

def add_survey_end_rows(df: pd.DataFrame, columns_to_keep: list) -> pd.DataFrame:
    """
    Adds a new row for each distinct sessionID where a SurveyTimestamp is present.
    The new row will have eventPage set to "PageIN", eventPara set to "SURVEY-END_PageIN_0",
    pageTitle set to "SURVEY-END", and eventTimestamp set to the value of SurveyTimestamp.
    The SurveyTimestamp column is then dropped.

    Parameters:
        df (pd.DataFrame): The original DataFrame.
        columns_to_keep (list): List of column names to retain in the resulting DataFrame.

    Returns:
        pd.DataFrame: The modified DataFrame with additional "SURVEY-END" rows and without the SurveyTimestamp column, ordered by.
    """

    # Create a copy of the original DataFrame
    df_copy = df.copy()

    # Filter rows where SurveyTimestamp is not null
    survey_rows = df_copy[df_copy['SurveyTimestamp'].notna()]

    # Group by sessionID and take the first occurrence of each sessionID
    survey_rows = survey_rows.drop_duplicates(subset='sessionID')

    # Create new rows with 'SURVEY-END' values
    new_rows = survey_rows.copy()
    new_rows['eventPage'] = 'PageIN'
    new_rows['eventPara'] = 'SURVEY-END_PageIN_0'
    new_rows['pageTitle'] = 'SURVEY-END'
    new_rows['eventTimestamp'] = new_rows['SurveyTimestamp']

    # Concatenate the new rows with the original DataFrame
    df_combined = pd.concat([df_copy, new_rows], ignore_index=True)

    # Drop the SurveyTimestamp column
    df_combined = df_combined.drop(columns=['SurveyTimestamp'])

    # Reorder columns to match the original order, if a specific order is provided
    df_combined = df_combined[columns_to_keep]

    df_combined = df_combined.sort_values(by = ["sessionID", "eventTimestamp"])

    return df_combined

def add_event_counts(df:pd.DataFrame, event_list:list) -> pd.DataFrame:
    """
    Adds columns to the dataframe for the count of specified events.
    
    Parameters:
        df (pandas.DataFrame): The input dataframe containing 'sessionID' and 'eventPage' columns.
        events (list): A list of event names to count and add as new columns.
    
    Returns:
        pandas.DataFrame: The dataframe with added event count columns.
    """
    
    for event in event_list:
        col_name = f'{event}_num'.lower()
        event_count = df[df['eventPage'] == event].groupby('sessionID').size().reset_index(name=col_name)
        df = df.merge(event_count, on='sessionID', how='left')
        df[col_name] = df[col_name].fillna(0).astype(int)
    
    return df

def calculate_total_time(df: pd.DataFrame, key_col:str, timestamp_col:str) -> pd.DataFrame:
    """
    Calculate the total time elapsed in hours and days for each CaseID.

    Parameters:
        df (pd.DataFrame): The input dataframe with columns key_col and timestamp_col.
        key_col (str): Name of the key column (case-id).
        timestamp_col (str): Name of the timestamp column.

    Returns:
        pd.DataFrame: A new dataframe with columns key_col, 'TotalTimeHH' (total time in hours) and 'TotalTimeDD'.
    """
    # Convert timestamp column to datetime if it's not already
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    
    # Calculate the difference between the max and min timestamp for each CaseID
    df_grouped = df.groupby(key_col).agg({timestamp_col: ['min', 'max'], key_col: 'size'})
    df_grouped.columns = ['StartTime', 'EndTime', 'CaseLength']
    df_grouped['TotalTime'] = df_grouped['EndTime'] - df_grouped['StartTime']
    
    # Convert total time to hours and days
    df_grouped['TotalTimeHH'] = (df_grouped['TotalTime'].dt.total_seconds() / 3600).round(2)    # add .astype(int) to have integer data
    df_grouped['TotalTimeMM'] = (df_grouped['TotalTime'].dt.total_seconds() / 60).round(2)      # add .astype(int) to have integer data
    df_grouped['TotalTimeDD'] = (df_grouped['TotalTime'].dt.total_seconds() / 86400).round(2)   # add .astype(int) to have integer data
    
    # Create the final dataframe
    result_df = df_grouped[['TotalTimeHH', 'TotalTimeMM', 'TotalTimeDD', 'CaseLength']].reset_index()
    
    return result_df

# Class functions
def add_class(timestamp: datetime, criteria: list) -> str:
    """
    Assign a class based on the timestamp and given criteria.

    Parameters:
        timestamp (datetime): The datetime of the event.
        criteria (list): A list of dictionaries containing 'date', 'start_time', 'end_time', and 'class'.

    Returns:
        str: The assigned class if the timestamp matches any criteria, otherwise None.
    """
    date = timestamp.date()
    time_of_day = timestamp.time()
    
    for criterion in criteria:
        if date == criterion['date']:
            if criterion['start_time'] <= time_of_day <= criterion['end_time']:
                return criterion['class']
    return "NA"

def plot_distinct_sessionID_per_class(df: pd.DataFrame, class_column: str, session_column: str, output_folder: Path):
    """
    Plots a vertical bar chart showing the count of distinct sessionIDs for each class in a DataFrame and saves the chart as an image in the specified folder.

    Parameters:
    - df: pd.DataFrame
        The input DataFrame containing the data.
    - class_column: str
        The name of the column in the DataFrame that represents the class labels.
    - session_column: str
        The name of the column in the DataFrame that contains the session IDs.
    - output_folder: Path
        The folder where the chart image will be saved (must already exist).

    Returns:
    - None
        Saves the bar chart as an image in the specified folder.
    """

    # Group by the class column and count distinct session IDs for each class
    class_counts = df.groupby(class_column)[session_column].nunique()

    # Create the vertical bar chart
    ax = class_counts.plot(kind='bar')

    # Add labels and title to the chart
    ax.set_xlabel('Class')
    ax.set_ylabel('Distinct sessionID Count')
    ax.set_title('Count of Distinct sessionID per Class')
    plt.xticks(rotation=0)  # Keep x-axis labels horizontal

    # Define the output file path
    output_file = Path(output_folder) / 'class_distinct_session_counts.png'

    # Save the chart as an image file
    plt.savefig(output_file, format='png')

    print(f"Chart saved successfully at: {output_file}")

    # Clear the figure to prevent overlapping in future plots
    plt.clf()

def save_distinct_sessionID_per_class(df: pd.DataFrame, class_column: str, session_column: str, output_folder: Path):
    """
    Saves a CSV file containing the count of distinct sessionIDs for each class in the DataFrame, along with the percentage of each class relative to the total number of distinct sessionIDs.

    Parameters:
    - df: pd.DataFrame
        The input DataFrame containing the data.
    - class_column: str
        The name of the column in the DataFrame that represents the class labels.
    - session_column: str
        The name of the column in the DataFrame that contains the session IDs.
    - output_folder: Path
        The folder where the CSV file will be saved.

    Returns:
    - None
        Saves the resulting class counts and percentages as a CSV and XLSX file in the specified folder.
    """

    # Group by the class column and count distinct session IDs for each class
    class_counts = df.groupby(class_column)[session_column].nunique().reset_index()

    # Rename columns for clarity in the CSV
    class_counts.columns = ['Class', 'Qty']

    # Calculate the total number of distinct sessionIDs
    total_qty = class_counts['Qty'].sum()

    # Calculate the percentage for each class
    class_counts['Perc'] = round((class_counts['Qty'] / total_qty),2) * 100

    class_counts = class_counts.sort_values(by='Qty', ascending=False)

    # Create the file path for saving the CSV
    output_file = Path(output_folder) / 'class_distinct_session_counts.csv'

    # Save the DataFrame to a CSV file
    class_counts.to_csv(output_file, index=False, sep=";")
    print(f"CSV file saved successfully at: {output_file}")
    print()

    output_file = Path(output_folder) / 'class_distinct_session_counts.xlsx'
    class_counts.to_excel(output_file, index=False, sheet_name="class_distinct_session_counts")
    print(f"XLSX file saved successfully at: {output_file}")
    print()
    
def save_distinct_eventTimestamps_for_na_class(df: pd.DataFrame, timestamp_column: str, class_column: str, output_folder: Path):
    """
    Saves a CSV file containing the distinct values of eventTimestamps for rows where the Class is 'NA',
    sorted from the oldest to the most recent timestamp.

    Parameters:
    - df: pd.DataFrame
        The input DataFrame containing the data.
    - timestamp_column: str
        The name of the column in the DataFrame that contains the event timestamps.
    - class_column: str
        The name of the column in the DataFrame that represents the class labels.
    - output_folder: Path
        The folder where the CSV file will be saved (must already exist).

    Returns:
    - None
        Saves the resulting distinct eventTimestamps as a CSV file in the specified folder.
    """

    # Filter the DataFrame for rows where Class is 'NA'
    na_class_df = df[df[class_column] == 'NA']

    # Extract distinct values of eventTimestamp and sort them
    distinct_timestamps = na_class_df[timestamp_column].drop_duplicates().sort_values()

    # Convert to DataFrame for saving
    distinct_timestamps_df = pd.DataFrame(distinct_timestamps, columns=[timestamp_column]).reset_index(drop=True)
    distinct_timestamps_df['eventDate'] = pd.to_datetime(distinct_timestamps_df[timestamp_column]).dt.date

    # Define the output file path
    output_file = Path(output_folder) / 'distinct_event_timestamps_na_class.csv'
    # Save the DataFrame to a CSV file
    distinct_timestamps_df.to_csv(output_file, index=True)
    print(f"CSV file saved successfully at: {output_file}")

    # Save the DataFrame to a CSV file
    output_file = Path(output_folder) / 'distinct_event_timestamps_na_class.xlsx'
    distinct_timestamps_df.to_excel(output_file, index=True, sheet_name="event_timestamps_na_class")
    print(f"XLSX file saved successfully at: {output_file}")

    # list_event_date = distinct_timestamps_df['eventDate'].to_list()
    # print(list_event_date)
    
    print()

# Tercile functions
def label_terciles_by_session(df: pd.DataFrame, session_column: str, value_column: str):
    """
    Label rows in terciles based on the value_column, considering all rows with the same session_column value as belonging to the same tercile, and add the 'Tercile' column to the original DataFrame.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame containing the data.
    session_column (str): The column representing session IDs (grouping key).
    value_column (str): The column containing the values to be split into terciles.
    
    Returns:
    pd.DataFrame: The original DataFrame with an additional column 'Tercile' indicating the tercile label.
    string: Name of the new tercile column
    """

    # Define the tercile column name
    col_tercile = f"{value_column}_Tercile"

    # First, remove duplicates based on session_column and value_column, because same session has same SUS
    df_unique = df.drop_duplicates(subset=[session_column, value_column])
    
    # Filter out rows where the value_column is NaN
    df_unique_non_nan = df_unique[df_unique[value_column].notna()]
    
    # Calculate terciles based on the unique non-NaN values
    df_unique_non_nan[col_tercile] = pd.qcut(df_unique_non_nan[value_column], q=3, labels=[1, 2, 3], duplicates='drop')
    
    # Merge the tercile labels back into the original dataframe
    df = df.merge(df_unique_non_nan[[session_column, col_tercile]], on=session_column, how='left')

    # Count the number of empty cells in the specified column
    num_empty = df[col_tercile].isna().sum()
    print(f"Number of empty cells in '{col_tercile}':", num_empty)

    # Add 0 as a category to allow setting empty cells to 0
    df[col_tercile] = df[col_tercile].cat.add_categories([0])

    # Replace empty values with 0 in the specified column
    df[col_tercile].fillna(0, inplace=True)

    return df, col_tercile

### MAIN ###
def main():
    print()
    print("*** PROGRAM START ***")
    print()

    start_time = datetime.now().replace(microsecond=0)
    print("Start process:", str(start_time))
    print()

    ### Events from DISCO ###
    df_disco = pd.DataFrame()
    df_disco_list = []
    path_disco_cases = Path(data_dir) / disco_cases
    if path_disco_cases.exists():
        print("Reading DISCO cases")
        df_disco = pd.read_csv(path_disco_cases)
        print("Cases in DISCO filter:", df_disco["Case ID"].nunique())
    else:
        print("Cases in DISCO filter: 0")
    print()

    df_disco_list = df_disco["Case ID"].unique().tolist()
    # print(df_disco_list) # debug

    ### Events from tutorial ###
    print(">> Reading Events data")
    path_events = Path(data_dir) / events_file
    print("Path:", str(path_events))
    col_list = ["sessionID","lang","pageName","pageTitle","menu","pageOrder","pagePara","event","duration","lastUpdate"]
    df_events = df_read_csv_data(path_events, col_list)
    col_list_unique = ["pageName","pageTitle","menu","pageOrder","pagePara","event"]
    # df_events_unique = df_get_unique_values(df_events, col_list_unique)
    # dict_with_formatting(df_events_unique)

    # Renaming 
    for key in dic_en_pageTitle:
        df_events['pageTitle'] = df_events['pageTitle'].replace([key], dic_en_pageTitle[key])

    for key in dic_en_event:
        df_events['event'] = df_events['event'].replace([key], dic_en_event[key])

    ### Create a list of distinct values to check the data (and print it) ###
    df_events_unique = df_get_unique_values(df_events, col_list_unique)
    dict_with_formatting(df_events_unique) 

    ### Create and event log at page-level (column "event", value "PageIN") ###
    print(">> Creating event log at page level")
    col_log = ["sessionID", "pageTitle", "menu", "pageOrder", "pagePara", "event", "lastUpdate"]
    # Specific for PAGE level
    # Filter the DataFrame for rows where 'event' is 'PageIN' (od click/dbclick for frequency)
    event_list = ['PageIN'] + clik_event_list
    df_log_page = df_events.loc[df_events['event'].isin(event_list)]
    df_log_page = df_retain_columns(df_log_page, col_log)
    # Common to PAGE / PARA level
    # Define a dictionary for renaming columns
    rename_dict = {'event': 'eventPage','lastUpdate': 'eventTimestamp'}
    df_log_page = df_rename_columns(df_log_page, rename_dict)
    print("> Fix duplicated timestamp")
    df_log_page['eventTimestamp'] = pd.to_datetime(df_log_page['eventTimestamp'])
    df_log_page = find_and_fix_ts_duplicates(df_log_page)
    # Adds the number of clicks and double clicks for each sessionID
    df_log_page = add_event_counts(df_log_page, clik_event_list)
    # Removes click/dbclick events
    df_log_page = df_remove_rows_with_substring(df_log_page, clik_event_list, ["eventPage", "eventPara"])
    # Show th final data
    df_show_data(df_log_page)
    print()

    ### Create and event log at para-level (column "event") ###
    print(">> Creating event log at para level")
    col_log = ["sessionID", "pageTitle", "menu", "pageOrder", "pagePara", "event", "lastUpdate", "eventPara"]
    # Specific for PARA level
    df_log_para = add_event_para_column(df_events)
    df_log_para = df_retain_columns(df_log_para, col_log)
    # Common to PAGE / PARA level
    # Define a dictionary for renaming columns
    rename_dict = {'event': 'eventPage','lastUpdate': 'eventTimestamp'}
    df_log_para = df_rename_columns(df_log_para, rename_dict)
    print("> Fix duplicated timestamp")
    df_log_para['eventTimestamp'] = pd.to_datetime(df_log_para['eventTimestamp'])
    df_log_para = find_and_fix_ts_duplicates(df_log_para)
    # Adds the number of clicks and double clicks for each sessionID
    df_log_para = add_event_counts(df_log_para, clik_event_list)
    # Removes click/dbclick events
    df_log_para = df_remove_rows_with_substring(df_log_para, clik_event_list, ["eventPage", "eventPara"])
    # Show th final data
    df_show_data(df_log_para)
    print()

    ### Quiz ###
    print(">> Reading Quiz data")
    path_quiz = Path(stats_dir) / quiz_stats_file
    print("Path:", str(path_quiz))
    col_list = ["sessionID", "QuizSessionCount", "QuizAnswerCorrectTotal", "QuizAnswerWrongTotal", "QuizAnswerCorrectRatioOverCount", "QuizAnswerCorrectRatioOverAll", "QuizSessionCount_P3","QuizAnswerCorrectTotal_P3","QuizAnswerWrongTotal_P3","QuizAnswerCorrectRatioOverCount_P3","QuizAnswerCorrectRatioOverAll_P3"]
    df_quiz = df_read_csv_data(path_quiz, col_list, ";")
    print(df_quiz.head())
    print()

    #### Survey ###
    print(">> Reading Survey data")
    path_survey = Path(data_dir) / survey_file_clean
    print("Path:", str(path_survey))
    df_survey = df_read_csv_data(path_survey, None, ";")
    print(df_survey.head())
    print()

    # Survey
    print(">> Reading SUS data")
    path_sus = Path(data_dir) / sus_file
    print("Path:", str(path_sus))
    col_list_sus = ["sessionID", "SUS", "Apprendimento percepito", "UEQ - Pragmatic", "UEQ - Hedonic", "UEQ - Overall"]
    df_sus = df_read_csv_data(path_sus, col_list_sus, ";")
    col_list_sus.pop(0) # remove "sessionID"
    for col in col_list_sus:
        df_sus[col] = df_sus[col].str.replace(',', '.')
        df_sus[col] = df_sus[col].fillna("0")
        df_sus[col] = df_sus[col].astype(float).round(3)
    print(df_sus.head())
    print()

    ### Merge with Quiz and Survey###
    print(">> Merging PAGE event log with Quiz and Survey")
    print("> Merging page level event log with Quiz and Survey")
    df_log_merge_1_page =  pd.merge(df_log_page, df_quiz, on='sessionID', how='left')
    df_log_merge_2_page =  pd.merge(df_log_merge_1_page, df_survey, on='sessionID', how='left')
    df_show_data(df_log_merge_2_page)
    print()
    distinct_session_count_1 = count_distinct_sessions_by_title(df_log_merge_2_page, "SURVEY")
    print("Number of distinct sessionID with survey (page levle):", distinct_session_count_1)
    print()

    print("> Merging PARA event log with Quiz and Survey")
    df_log_merge_1_para =  pd.merge(df_log_para, df_quiz, on='sessionID', how='left')
    df_log_merge_2_para =  pd.merge(df_log_merge_1_para, df_survey, on='sessionID', how='left')    
    df_show_data(df_log_merge_2_para)
    print()
    distinct_session_count_2 = count_distinct_sessions_by_title(df_log_merge_2_para, "SURVEY")
    print("Number of distinct sessionID with survey (para level):", distinct_session_count_2)
    print()

    ### Merge both df with SUS etc ###
    print("> Merging PAGE and PARA event log with SUS")
    df_log_merge_2_page =  pd.merge(df_log_merge_2_page, df_sus, on='sessionID', how='left')
    df_log_merge_2_para =  pd.merge(df_log_merge_2_para, df_sus, on='sessionID', how='left')


    ### Final event log with survey end as event ###
    print(">> Creating final event log with survey responses as event")

    # Final list of columns in the event log
    columns_to_keep = ['sessionID', 'pageTitle', 'menu', 'pageOrder', 'pagePara', 'eventPage','eventTimestamp', 'eventPara', 'click_num', 'dbclick_num',
                    'QuizSessionCount', 'QuizAnswerCorrectTotal', 'QuizAnswerWrongTotal',  'QuizAnswerCorrectRatioOverCount', 'QuizAnswerCorrectRatioOverAll', 'QuizSessionCount_P3','QuizAnswerCorrectTotal_P3','QuizAnswerWrongTotal_P3','QuizAnswerCorrectRatioOverCount_P3','QuizAnswerCorrectRatioOverAll_P3',
                    'Q_1', 'Q_2', 'Q_3', 'Q_4', 'Q_5', 'Q_6', 'Q_7', 'Q_8', 'Q_9', 'Q_10', 'Q_11', 'Q_12', 'Q_13', 'Q_14', 'Q_15', 
                    'Q_16', 'Q_17', 'Q_18', 'Q_19', 'Q_20', 'Q_21', 'Q_22', 'Q_23', 'Q_24', 'Q_25', 'Q_26', 'Q_27', 'Q_28'] + col_list_sus
    print(f"Columns in the vent log ({len(columns_to_keep)}): ", columns_to_keep)
    
    df_log_merge_2_page_final = add_survey_end_rows(df_log_merge_2_page, columns_to_keep)
    df_log_merge_2_page_final = df_log_merge_2_page_final.drop('eventPara', axis=1) # the page level has no para level

    df_log_merge_2_para_final = add_survey_end_rows(df_log_merge_2_para, columns_to_keep)

    # Setting integer columns
    print("> Setting integer columns")
    # Convert specified columns to integers
    columns_to_convert = ['click_num', 'dbclick_num','QuizSessionCount','QuizAnswerCorrectTotal','QuizAnswerWrongTotal']
    # Converting the columns to integers, setting errors='coerce' to handle non-convertible values
    df_log_merge_2_page_final[columns_to_convert] = df_log_merge_2_page_final[columns_to_convert].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
    df_log_merge_2_para_final[columns_to_convert] = df_log_merge_2_para_final[columns_to_convert].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
    print()

    # Add total time and case length
    print("> Computing total times")
    df_log_merge_2_page_total_time = calculate_total_time(df_log_merge_2_page_final, "sessionID", "eventTimestamp")
    df_log_merge_2_page_total_time = df_log_merge_2_page_total_time.sort_values(by=["TotalTimeHH", "TotalTimeMM", "TotalTimeDD","CaseLength","sessionID"])
    
    df_log_merge_2_para_total_time = calculate_total_time(df_log_merge_2_para_final, "sessionID", "eventTimestamp")
    df_log_merge_2_para_total_time = df_log_merge_2_para_total_time.sort_values(by=["TotalTimeHH", "TotalTimeMM", "TotalTimeDD","CaseLength","sessionID"])

    # Saving
    path_out = Path(stats_dir) / "edu_event_log_PAGE_raw_total_time.csv"
    print("Saving total times (PAGE) to:", path_out)
    df_log_merge_2_page_total_time.to_csv(path_out, sep=";", index=False)

    path_out = Path(stats_dir) / "edu_event_log_PARA_raw_total_time.csv"
    print("Saving total times (PAGE) to:", path_out)
    df_log_merge_2_para_total_time.to_csv(path_out, sep=";", index=False)

    # Merge final data with total times for stats
    df_log_merge_2_page_final = pd.merge(df_log_merge_2_page_final, df_log_merge_2_page_total_time, on="sessionID", how="left")
    df_log_merge_2_para_final = pd.merge(df_log_merge_2_para_final, df_log_merge_2_para_total_time, on="sessionID", how="left")

    df_log_merge_2_page_final = df_log_merge_2_page_final.sort_values(by=["TotalTimeHH", "TotalTimeMM", "TotalTimeDD", "CaseLength","sessionID"])
    df_log_merge_2_para_final = df_log_merge_2_para_final.sort_values(by=["TotalTimeHH", "TotalTimeMM", "TotalTimeDD", "CaseLength","sessionID"])

    # Adds the class
    print(">> Adding classes")
    df_log_merge_2_page_final['Class'] = df_log_merge_2_page_final['eventTimestamp'].apply(lambda x: add_class(x, criteria))
    df_log_merge_2_para_final['Class'] = df_log_merge_2_para_final['eventTimestamp'].apply(lambda x: add_class(x, criteria))
    print(">> Stats about classes")
    plot_distinct_sessionID_per_class(df_log_merge_2_page_final, "Class", "sessionID", plots_dir)
    save_distinct_sessionID_per_class(df_log_merge_2_page_final, "Class", "sessionID", stats_dir)
    save_distinct_eventTimestamps_for_na_class(df_log_merge_2_page_final, "eventTimestamp", "Class", stats_dir)

    # Adds the class to quiz stats
    print(">> Updating Quiz ratio totals with Class")
    df_quiz = df_quiz.merge(df_log_merge_2_page_final[['sessionID', 'Class']], on='sessionID', how='left')
    df_quiz = df_quiz.drop_duplicates()
    print(df_quiz.head(5))
    print()
    path_out = Path(stats_dir) / quiz_stats_file
    # CSV
    print("Path (CSV):", path_out)
    df_quiz.to_csv(path_out, sep=";", index=False)
    # XLS
    path_out = Path(stats_dir) / f"{Path(quiz_stats_file).stem}.xlsx"
    print("Path (XLSX):", path_out)
    df_quiz.to_excel(path_out, sheet_name=f"{Path(quiz_stats_file).stem}", index=False)
    print()

    df_log_merge_2_page_final = df_log_merge_2_page_final.drop_duplicates()
    df_log_merge_2_para_final = df_log_merge_2_para_final.drop_duplicates()

    print("Log at PAGE level")
    df_show_data(df_log_merge_2_page_final)
    print()

    print("Log at PARA level")
    df_show_data(df_log_merge_2_para_final)
    print()

    ### Renaming "sessionID" as "Case ID" ###
    df_log_merge_2_page_final.rename(columns={"sessionID": id_column}, inplace=True)
    df_log_merge_2_para_final.rename(columns={"sessionID": id_column}, inplace=True)
    
    ### Renaming "eventTimestamp" as "Complete Timestamp" ###
    df_log_merge_2_page_final.rename(columns={"eventTimestamp": timestamp_column}, inplace=True)
    df_log_merge_2_para_final.rename(columns={"eventTimestamp": timestamp_column}, inplace=True)

    ### Add activity column based on dataframe ###
    # pageTitle -> Activity
    df_log_merge_2_page_final.insert(1, activity_column, df_log_merge_2_page_final["pageTitle"])
    column = df_log_merge_2_page_final.pop(activity_column)
    df_log_merge_2_page_final.insert(2, activity_column, column)
    # eventPara -> Activity
    df_log_merge_2_para_final.insert(1, activity_column, df_log_merge_2_para_final["eventPara"])
    column = df_log_merge_2_para_final.pop(activity_column)
    df_log_merge_2_para_final.insert(2, activity_column, column)

    ### Ordering ###
    df_log_merge_2_page_final.sort_values(by=[id_column, timestamp_column], ascending=[True, True], inplace=True)
    df_log_merge_2_para_final.sort_values(by=[id_column, timestamp_column], ascending=[True, True], inplace=True)

    ### Adding Terciles ###
    print(">> Adding Terciles")
    list_col_t = ["SUS", "Apprendimento percepito", "UEQ - Overall", "QuizAnswerCorrectRatioOverAll"] # Columns on which to calculate the tertile
    print("Columns on which to calculate the tercile:", list_col_t)
    for col_name in list_col_t:
        print("Tercile on column:", col_name)
        df_log_merge_2_page_final, col_tercile = label_terciles_by_session(df_log_merge_2_page_final, session_column=id_column, value_column=col_name)
        df_log_merge_2_para_final, col_tercile = label_terciles_by_session(df_log_merge_2_para_final, session_column=id_column, value_column=col_name)
        print("New tercile column:", col_tercile)
        # print("Event log shape:", df_log.shape)
        print("Event log new tercile (PAGE):", df_log_merge_2_page_final[col_tercile].unique())
        print("Event log new tercile (PARA):", df_log_merge_2_para_final[col_tercile].unique())
        print()

    ### Saving ###
    print("> Saving raw data")
    path_out = Path(log_dir) / "edu_event_log_PAGE_raw_ter.csv"
    print("Saving final event log to:", path_out)
    df_log_merge_2_page_final.to_csv(path_out, sep=";", index=False)
    
    path_out = Path(log_dir) / "edu_event_log_PARA_raw_ter.csv"
    print("Saving final event log to:", path_out)
    df_log_merge_2_para_final.to_csv(path_out, sep=";", index=False)
    print()

    ### Filter based on DISCO ###
    if filter_disco_cases == 1:
        print("> Filtering Cases already chosen in DISCO")
        df_log_merge_2_para_final = df_log_merge_2_para_final[df_log_merge_2_para_final[id_column].isin(df_disco_list)]
        print("Cases after DISCO filter (para):", df_log_merge_2_para_final[id_column].nunique())
        df_log_merge_2_page_final = df_log_merge_2_page_final[df_log_merge_2_page_final[id_column].isin(df_disco_list)]
        print("Cases after DISCO filter (page):", df_log_merge_2_page_final[id_column].nunique())
        print()

        print("> Saving filtered data")
        path_out = Path(log_dir) / "edu_event_log_PAGE_raw_filtered_DISCO_ter.csv"
        print("Saving final event log to:", path_out)
        df_log_merge_2_page_final.to_csv(path_out, sep=";", index=False)
        
        path_out = Path(log_dir) / "edu_event_log_PARA_raw_filtered_DISCO_ter.csv"
        print("Saving final event log to:", path_out)
        df_log_merge_2_para_final.to_csv(path_out, sep=";", index=False)
        print()

    # Extract lines where 'CaseLen' > case_len_threshold
    # df_log_merge_2_page_final = df_log_merge_2_page_final[(df_log_merge_2_page_final['CaseLength'] > case_len_threshold) & (df_log_merge_2_page_final['TotalTimeHH'] < case_time_threshold)]
    # df_log_merge_2_para_final = df_log_merge_2_para_final[(df_log_merge_2_para_final['CaseLength'] > case_len_threshold) & (df_log_merge_2_para_final['TotalTimeHH'] < case_time_threshold)]

    # program END
    end_time = datetime.now().replace(microsecond=0)
    delta_time = end_time - start_time

    print()
    print("End process:", end_time)
    print("Time to finish:", delta_time)

    print()
    print("*** PROGRAM END ***")
    print()


if __name__ == "__main__":
    main()
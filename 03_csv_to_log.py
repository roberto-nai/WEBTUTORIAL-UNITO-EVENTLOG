# 03_csv_to_log.py

### IMPORT ###
from pathlib import Path
import csv
from datetime import datetime
import pandas as pd

### LOCAL IMPORT ###
from config import config_reader
from utilities import df_read_csv_data, df_get_unique_values, df_show_data, df_retain_columns, df_rename_columns, dict_with_formatting 

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
# print(yaml_config) # debug
data_dir = str(yaml_config["DATA_DIR"])
log_dir = str(yaml_config["LOG_DIR"])
stats_dir = str(yaml_config["STATS_DIR"])
events_file = str(yaml_config["EVENTS_FILE"]) # input
quiz_stats_file = str(yaml_config["QUIZ_STATS_FILE"]) # input
survey_file_clean = str(yaml_config["SURVEY_GOOGLE_FILE_CLEAN"]) # input

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

def extract_distinct_menu_per_session(df: pd.DataFrame, key_column: str, menu_column: str) -> pd.DataFrame:
    """
    Extracts the distinct values of the menu column for each distinct sessionID from a dataframe.

    Parameters:
        df (pd.DataFrame): The dataframe containing the data.
        key_column (str): The column name to group by (typically sessionID).
        menu_column (str): The column name from which to extract distinct values (typically menu).

    Returns:
        pd.DataFrame: A dataframe with each sessionID and the distinct values of the menu column.
    """

    # Group by the key column and aggregate distinct menu values
    grouped_df = df.groupby(key_column)[menu_column].apply(lambda x: list(x.unique())).reset_index()
    
    # Rename the columns for clarity
    grouped_df.columns = [key_column, 'DistinctMenuValues']

    # Sort
    grouped_df = grouped_df.sort_values(by = "DistinctMenuValues")
    
    return grouped_df

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


### MAIN ###
def main():
    print()
    print("*** PROGRAM START ***")
    print()

    start_time = datetime.now().replace(microsecond=0)
    print("Start process:", str(start_time))
    print()

    # Events
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

    # Create a list of distinct values to check the data (and print it)
    df_events_unique = df_get_unique_values(df_events, col_list_unique)
    dict_with_formatting(df_events_unique) 

    # Create and event log at page-level (column "event", value "PageIN")
    print(">> Creating event log at page level")
    col_log = ["sessionID", "pageTitle", "menu", "pageOrder", "pagePara", "event", "lastUpdate"]
    # Filter the DataFrame for rows where 'event' is 'PageIN'
    df_log_page = df_events.loc[df_events['event'] == 'PageIN']
    df_log_page = df_retain_columns(df_log_page, col_log)
    # Define a dictionary for renaming columns
    rename_dict = {'event': 'eventPage','lastUpdate': 'eventTimestamp'}
    df_log_page = df_rename_columns(df_log_page, rename_dict)
    print("> Fix duplicated timestamp")
    df_log_page['eventTimestamp'] = pd.to_datetime(df_log_page['eventTimestamp'])
    df_log_page = find_and_fix_ts_duplicates(df_log_page)
    # Adds the number of clicks and double clicks for each sessionID
    df_log_page = add_event_counts(df_log_page, clik_event_list)
    # Show th final data
    df_show_data(df_log_page)
    print()

    # Create and event log at para-level (column "event")
    print(">> Creating event log at para level")
    col_log = ["sessionID", "pageTitle", "menu", "pageOrder", "pagePara", "event", "lastUpdate", "eventPara"]
    df_log_para = add_event_para_column(df_events)
    df_log_para = df_retain_columns(df_log_para, col_log)
    # Define a dictionary for renaming columns
    rename_dict = {'event': 'eventPage','lastUpdate': 'eventTimestamp'}
    df_log_para = df_rename_columns(df_log_para, rename_dict)
    print("> Fix duplicated timestamp")
    df_log_para['eventTimestamp'] = pd.to_datetime(df_log_para['eventTimestamp'])
    df_log_para = find_and_fix_ts_duplicates(df_log_para)
    # Adds the number of clicks and double clicks for each sessionID
    df_log_para = add_event_counts(df_log_para, clik_event_list)
    # Show th final data
    df_show_data(df_log_para)
    print()

    # Quiz
    print(">> Reading Quiz data")
    path_quiz = Path(stats_dir) / quiz_stats_file
    print("Path:", str(path_quiz))
    col_list = ["sessionID","QuizSessionCount", "QuizAnswerCorrectTotal", "QuizAnswerWrongTotal", "QuizAnswerCorrectRatio"]
    df_quiz = df_read_csv_data(path_quiz, col_list, ";")
    print(df_quiz.head())
    print()

    # Survey
    print(">> Reading Survey data")
    path_survey = Path(data_dir) / survey_file_clean
    print("Path:", str(path_survey))
    df_survey = df_read_csv_data(path_survey, None, ";")
    print(df_survey.head())
    print()

    # Merge
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

    # Final event log with survey end as event
    print(">> Creating final event log with survey responses as event")

    # Final list of columns in the event log
    columns_to_keep = ['sessionID', 'pageTitle', 'menu', 'pageOrder', 'pagePara', 'eventPage','eventTimestamp', 'eventPara', 'click_num', 'dbclick_num',
                    'QuizSessionCount', 'QuizAnswerCorrectTotal', 'QuizAnswerWrongTotal',  'QuizAnswerCorrectRatio', 
                    'Q_1', 'Q_2', 'Q_3', 'Q_4', 'Q_5', 'Q_6', 'Q_7', 'Q_8', 'Q_9', 'Q_10', 'Q_11', 'Q_12', 'Q_13', 'Q_14', 'Q_15', 
                    'Q_16', 'Q_17', 'Q_18', 'Q_19', 'Q_20', 'Q_21', 'Q_22', 'Q_23', 'Q_24', 'Q_25', 'Q_26', 'Q_27', 'Q_28']

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

    df_show_data(df_log_merge_2_page_final)
    print()

    df_show_data(df_log_merge_2_para_final)
    print()
    
    # Saving
    print("> Saving")
    print()
    path_out = Path(log_dir) / "edu_event_log_PAGE_raw.csv"
    print("Saving final event log to:", path_out)
    df_log_merge_2_page_final.to_csv(path_out, sep=";", index=False)
    
    path_out = Path(log_dir) / "edu_event_log_PARA_raw.csv"
    print("Saving final event log to:", path_out)
    df_log_merge_2_para_final.to_csv(path_out, sep=";", index=False)
    print()

    print(">> Getting path menu by sessionID")
    df_menu = extract_distinct_menu_per_session(df_log_merge_2_page_final, "sessionID", "menu")
    path_out = Path(stats_dir) / "menu_stats.csv"
    print("Saving menu list:", path_out)
    df_menu.to_csv(path_out, sep=";", index=False)
    print()

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
"""
01_survey_clean.py
"""

### IMPORT ###
from pathlib import Path
from datetime import datetime
import pandas as pd
import csv

### LOCAL IMPORT ###
from config import config_reader

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
# print(yaml_config) # debug
data_dir = str(yaml_config["DATA_DIR"])
stats_dir = str(yaml_config["STATS_DIR"])
survey_file = str(yaml_config["SURVEY_GOOGLE_FILE"]) # input
survey_file_clean = str(yaml_config["SURVEY_GOOGLE_FILE_CLEAN"]) # output
survey_file_clean_map = str(yaml_config["SURVEY_GOOGLE_FILE_CLEAN_MAP"]) # output
survey_key_col = str(yaml_config["SURVEY_GOOGLE_KEY_COLUMN"]) 
survey_file_stats = str(yaml_config["SURVEY_GOOGLE_FILE_STATS"]) 

### FUNCTIONS ###

def survey_read_and_rename_columns(df:pd.DataFrame, key_col:str) -> pd.DataFrame:
    """
    Reads a CSV file, removes rows where the 'Form blueprint' column is empty, truncates it up to the 'Form blueprint' column inclusive, renames the first column (post-truncation) to 'sessionID', the next column to 'eventTimestamp' (survey timestamp), and the remaining columns sequentially from 1. 
    Additionally, it converts the timestamp from the format 'YYYY/MM/DD HH:MM:SS AM/PM TZ' to 'YYYY-MM-DD HH:MM:SS'.
    Finally, it returns a mapping of the original column names (questions) mapped in Q_i.

    Parameters:
        df (pd.DataFrame): The dataframe to clean.
        key_col (str): The key column name.

    Returns:
        pd.DataFrame: A pandas DataFrame with renamed columns and without empty rows in the 'Form blueprint' column.
        pd.DataFrame: A pandas DataFrame with a mapping of the original column names (questions) mapped in Q_i.
    """
    
    # Remove rows where the 'Form blueprint' column (need to specify the exact name) is empty
    df = df.dropna(subset=[key_col])
    df = df.drop_duplicates()

    # Find the index of the column 'Form blueprint'
    try:
        # We need to adjust the column name to exactly match how it appears in the file, including any trailing characters
        blueprint_index = df.columns.get_loc(key_col) + 1  # Add 1 to adjust for Python's 0-based indexing
    except KeyError:
        print(f"Key column '{key_col}' not found. Please check the exact column name in the CSV.")
        return None
    
    # Select columns up to and including 'Form blueprint'
    df = df.iloc[:, :blueprint_index]

    # Store the original column names
    original_columns = df.columns.tolist()
    
    # Rename the first column to 'sessionID', the next to 'eventTimestamp' and the others sequentially
    # df.columns = [f"QUESTION_{i+1}" for i in range(len(df.columns))]
    # column_names = ['SurveyTimestamp'] + [f"Q_{i}" for i in range(1, len(df.columns))] 
    column_names = ['SurveyTimestamp'] + [f"Q_{i}" for i in range(1, len(df.columns)-1)] + ['sessionID'] # list of column names

    df.columns = column_names

    # Move the 'sessionID' to the first position if it's not already the first (adjusting in case the original position changes)
    column_order = ['sessionID'] + [col for col in df.columns if col != 'sessionID']
    df = df[column_order]

    # Convert 'eventTimestamp' from 'YYYY/MM/DD HH:MM:SS AM/PM EET' to 'YYYY-MM-DD HH:MM:SS'
    # Remove the timezone information from 'SurveyTimestamp' and convert it
    
    # Remove the "EET" suffix from the string as it is not compatible with the datetime format
    df['SurveyTimestamp'] = df['SurveyTimestamp'].str.replace(' EET', '', regex=False)

    # Convert the string to datetime format, specifying the original timezone as "Europe/Helsinki"
    df['SurveyTimestamp'] = pd.to_datetime(df['SurveyTimestamp'], format='%Y/%m/%d %I:%M:%S %p')
    df['SurveyTimestamp'] = df['SurveyTimestamp'].dt.tz_localize('Europe/Helsinki')

    # Convert the timestamp to the Italian timezone "Europe/Rome" (handles daylight saving time automatically)
    df['SurveyTimestamp'] = df['SurveyTimestamp'].dt.tz_convert('Europe/Rome')

    # Convert the timestamp back to a string in the desired format
    df['SurveyTimestamp'] = df['SurveyTimestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Converts all float columns to int inserting -1 for missing values
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].fillna(-1).astype(int)

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna("-1").astype(str)

    # Create a DataFrame to map original column names to new column names
    columns_map_df = pd.DataFrame({
        'question_num': column_names,
        'question_text': original_columns
    })
        
    # Returns the new dataframe [sessionID, SurveyTimestamp, Q_i] and the mapped Q_i
    return df, columns_map_df

def get_unique_values(df:pd.DataFrame, prefix:str="Q_", start:int=1, end:int=28):
    """
    Retrieves unique values from a series of columns in a DataFrame that follow a specific naming pattern.

    Parameters:
        df (pd.DataFrame): The DataFrame to process.
        prefix (str): The common prefix of the column names to be checked (default is "Q_").
        start (int): The starting index of the column names (inclusive).
        end (int): The ending index of the column names (inclusive).

    Returns:
        dict: A dictionary with each key as the column name and the associated value as an array of unique values.
    """
    unique_values = {}

    # Iterate through each column from start to end index
    for i in range(start, end + 1):
        col_name = f"{prefix}{i}"
        # Check if the column exists in the DataFrame
        if col_name in df.columns:
            # Get unique values, sort them, and add them to the dictionary
            unique_values[col_name] = sorted(df[col_name].dropna().unique())

    return unique_values

def calculate_question_statistics(df: pd.DataFrame, question_texts_df: pd.DataFrame, question_prefix: str) -> pd.DataFrame:
    """
    Calculate the sum and percentage of distinct values for each question relative to all sessionIDs.

    Parameters:
        df (pd.DataFrame): DataFrame containing survey data with columns 'sessionID', 'SurveyTimestamp', and question columns.
        question_texts_df (pd.DataFrame): DataFrame containing survey questions.
        question_prefix (str): The prefix used to identify question columns (e.g., 'Q_').

    Returns:
        pd.DataFrame: A DataFrame with the question, question_value, sum of answers, and answer ratio.
    """
    
    # Identify question columns by filtering for columns that start with the specified prefix
    question_columns = [col for col in df.columns if col.startswith(question_prefix)]

    # List to collect output data
    output_data = []

    # Iterate over each question column
    for question in question_columns:
        # Group by question value, and count occurrences across all sessions
        grouped = df.groupby(question).size().reset_index(name='count')
        
        # Calculate the total number of responses across all sessions
        total_responses = df[question].count()
        
        # Calculate the sum and ratio for each distinct value of the question
        grouped['answer_sum'] = grouped['count']
        grouped['answer_ratio'] = (grouped['count'] / total_responses).round(3)
        
        # Add a column for the current question
        grouped['question_no'] = question
        
        # Rename the question value column for clarity
        grouped = grouped.rename(columns={question: 'answer_value'})
        
        # Select the desired columns and append to output list
        final = grouped[['question_no', 'answer_value', 'answer_sum', 'answer_ratio']]
        output_data.append(final)

    # Concatenate all results
    final_output = pd.concat(output_data, ignore_index=True)

    # Merge with the question texts DataFrame
    final_output_merged = pd.merge(final_output, question_texts_df, left_on="question_no", right_on="question_num", how="left")
    # print(final_output_merged.columns) # debug
        
    # Select and reorder columns for the final output
    final_df = final_output_merged[['question_no', 'question_text', 'answer_value', 'answer_sum', 'answer_ratio']]

    return final_df

### MAIN ###
def main():
    print()
    print("*** PROGRAM START ***")
    print()

    start_time = datetime.now().replace(microsecond=0)
    print("Start process:", str(start_time))
    print()


    # Survey
    print(">> Reading Survey data")
    print()
    path_survey = Path(data_dir) / survey_file
    print("Input file:", path_survey)
    df_survey = pd.read_csv(path_survey, sep=';')
    print("Raw data preview")
    print(df_survey.head())
    print()

    print(">> Cleaning Survey data")
    print()
    df_survey_clean, columns_map_df = survey_read_and_rename_columns(df_survey, survey_key_col)
    print("Clean data preview")
    print(df_survey_clean.head())
    print()
    print(df_survey_clean.dtypes)
    print()
    print(df_survey_clean.columns)
    print()
    path_out = Path(data_dir) / survey_file_clean
    print()
    print("Saving Survey data clean to:", path_survey)
    df_survey_clean.to_csv(path_out, sep=";", index=False,quoting=csv.QUOTE_NONNUMERIC)
    print()
    path_out = Path(data_dir) / survey_file_clean_map
    print("Saving Survey data clean to:", path_survey)
    columns_map_df.to_csv(path_out, sep=";", index=False,quoting=csv.QUOTE_NONNUMERIC)
    print()

    print(">> Distinct values in the Survey data clean")
    print()
    print("Unique Session ID:", df_survey_clean["sessionID"].nunique())
    print()
    unique_values = get_unique_values(df_survey_clean, "Q_", 1, 28)
    for key, value in unique_values.items():
        print(f"Unique values in column {key}: {value}")
    print()

    print(">> Stats about Survey data")
    print()
    print("Unique Session ID:", df_survey_clean["sessionID"].nunique())
    print()
    df_survey_stats = calculate_question_statistics(df_survey_clean, columns_map_df, "Q_")
    print(df_survey_stats.head())
    print()
    path_out = Path(stats_dir) / survey_file_stats
    print("Saving Survey stats in CSV:", path_out)
    df_survey_stats.to_csv(path_out, sep=";", index=False, quoting=csv.QUOTE_NONNUMERIC)
    # saving in XLSX
    survey_file_stats_xlsx = f"{Path(survey_file_stats).stem}.xlsx"
    path_out = Path(stats_dir) / survey_file_stats_xlsx
    print("Saving Survey stats in XLSX:", path_out)
    df_survey_stats.to_excel(path_out, index=False, sheet_name=f"{Path(survey_file_stats).stem}")

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
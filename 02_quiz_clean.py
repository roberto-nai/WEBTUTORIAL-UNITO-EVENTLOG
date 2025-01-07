"""
02_quiz_clean.py

Given the raw export of the quizzes, it generates a file with the results for each session-id
"""

### IMPORT ###
from pathlib import Path
from datetime import datetime
import pandas as pd

### LOCAL IMPORT ###
from config import config_reader
from utilities import df_read_csv_data

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
# print(yaml_config) # debug
data_dir = str(yaml_config["DATA_DIR"])
stats_dir = str(yaml_config["STATS_DIR"])
quiz_file = str(yaml_config["QUIZ_FILE"]) # input
quiz_stats_file = str(yaml_config["QUIZ_STATS_FILE"]) # output

### FUNCTIONS ###
def quiz_correct_ratio(df: pd.DataFrame, key_column: str, filter_list: list) -> pd.DataFrame:
    """
    Starting from a dataframe, it groups quiz results by sessionID

    Parameters:
        df (pd.DataFrame): The dataframe with data.
        key_column (str): The ID column name.
        filter_list (list): List of strings to filter the dataframe by pageTitle.
    Returns:
        pd.DataFrame: The dataframe with data grouped by.
    """
    grouped_df = df.groupby(key_column).agg(
        QuizSessionCount=pd.NamedAgg(column=key_column, aggfunc='count'),
        QuizAnswerCorrectTotal=pd.NamedAgg(column='answerCorrect', aggfunc='sum'),
        QuizAnswerWrongTotal=pd.NamedAgg(column='answerCorrect', aggfunc=lambda x: (x == 0).sum())
    ).reset_index()
    
    # Ensure columns as integer
    grouped_df['QuizSessionCount'] = grouped_df['QuizSessionCount'].astype(int)
    grouped_df['QuizAnswerCorrectTotal'] = grouped_df['QuizAnswerCorrectTotal'].astype(int)
    grouped_df['QuizAnswerWrongTotal'] = grouped_df['QuizAnswerWrongTotal'].astype(int)

    grouped_df['QuizAnswerCorrectRatioOverCount'] = (grouped_df['QuizAnswerCorrectTotal'] / grouped_df['QuizSessionCount']).round(2)
    grouped_df['QuizAnswerCorrectRatioOverAll'] = (grouped_df['QuizAnswerCorrectTotal'] / 10).round(2)

    # Filter the dataframe by pageTitle using the filter_list
    filtered_df = df[df['pageTitle'].isin(filter_list)]
    
    # Group the filtered dataframe
    filtered_grouped_df = filtered_df.groupby(key_column).agg(
        QuizSessionCount_P3=pd.NamedAgg(column=key_column, aggfunc='count'),
        QuizAnswerCorrectTotal_P3=pd.NamedAgg(column='answerCorrect', aggfunc='sum'),
        QuizAnswerWrongTotal_P3=pd.NamedAgg(column='answerCorrect', aggfunc=lambda x: (x == 0).sum())
    ).reset_index()
    
    # Ensure columns as integer for the filtered data
    filtered_grouped_df['QuizSessionCount_P3'] = filtered_grouped_df['QuizSessionCount_P3'].astype(int)
    filtered_grouped_df['QuizAnswerCorrectTotal_P3'] = filtered_grouped_df['QuizAnswerCorrectTotal_P3'].astype(int)
    filtered_grouped_df['QuizAnswerWrongTotal_P3'] = filtered_grouped_df['QuizAnswerWrongTotal_P3'].astype(int)

    filtered_grouped_df['QuizAnswerCorrectRatioOverCount_P3'] = (filtered_grouped_df['QuizAnswerCorrectTotal_P3'] / filtered_grouped_df['QuizSessionCount_P3']).round(2)
    filtered_grouped_df['QuizAnswerCorrectRatioOverAll_P3'] = (filtered_grouped_df['QuizAnswerCorrectTotal_P3'] / 3).round(2)
    
    # Merge the filtered_grouped_df with the original grouped_df on the key_column
    merged_df = grouped_df.merge(filtered_grouped_df, on=key_column, how='left')
    
    return merged_df


### MAIN ###
def main():
    print()
    print("*** PROGRAM START ***")
    print()

    start_time = datetime.now().replace(microsecond=0)
    print("Start process:", str(start_time))
    print()

    # List of prefixes
    print(">> Prefix list")
    prefix_list = ["Introduzione-Quiz", "Primo programma-Quiz", "Variabili-Quiz"] # first 3 common tracks
    print(f"Values: {len(prefix_list)}: {prefix_list}")
    print()

    # Quiz
    print(">> Reading Quiz data")
    path_quiz = Path(data_dir) / quiz_file
    print("Path:", str(path_quiz))
    col_list = ["sessionID","lang","pageName","pageTitle","menu","pageOrder","answer","answerCorrect","lastUpdate"]
    df_quiz = df_read_csv_data(path_quiz, col_list)
    
    # Quiz group by sessionID
    print("> Getting Quiz ratio totals")
    df_quiz_ratio = quiz_correct_ratio(df_quiz, "sessionID", prefix_list)
    print(df_quiz_ratio.head())
    print()

    print("> Saving Quiz ratio totals")
    path_out = Path(stats_dir) / quiz_stats_file
    # CSV
    print("Path (CSV):", path_out)
    df_quiz_ratio.to_csv(path_out, sep=";", index=False)
    # XLS
    path_out = Path(stats_dir) / f"{Path(quiz_stats_file).stem}.xlsx"
    print("Path (XLSX):", path_out)
    df_quiz_ratio.to_excel(path_out, sheet_name=f"{Path(quiz_stats_file).stem}", index=False)

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
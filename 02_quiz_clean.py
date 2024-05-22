# 02_quiz_clean.py

### IMPORT ###
from pathlib import Path
import csv
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
def quiz_correct_ratio(df: pd.DataFrame, key_column:str) -> pd.DataFrame:
    """
    Starting from a dataframe, it groups quiz results by sessionID

    Parameters:
        df (pd.DataFrame): The dataframe with data.
        key_column (str): The column name
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

    grouped_df['QuizAnswerCorrectRatio'] = (grouped_df['QuizAnswerCorrectTotal'] / grouped_df['QuizSessionCount']).round(2)
    
    return grouped_df

def extract_quiz_summary(df: pd.DataFrame, key_column: str) -> pd.DataFrame:
    """
    Extracts the quiz summary for each distinct sessionID from a dataframe.

    Parameters:
        df (pd.DataFrame): The dataframe containing the data.
        key_column (str): The column name to group by (typically sessionID).

    Returns:
        pd.DataFrame: A dataframe with the quiz summary for each distinct sessionID.
    """
    # Select the relevant columns
    summary_columns = [key_column, 'QuizSessionCount', 'QuizAnswerCorrectTotal', 'QuizAnswerCorrectRatio']
    
    # Drop duplicates to get distinct sessionID summaries
    summary_df = df[summary_columns].drop_duplicates()
    
    return summary_df

### MAIN ###
def main():
    print()
    print("*** PROGRAM START ***")
    print()

    start_time = datetime.now().replace(microsecond=0)
    print("Start process:", str(start_time))
    print()

    # Quiz
    print(">> Reading Quiz data")
    path_quiz = Path(data_dir) / quiz_file
    print("Path:", str(path_quiz))
    col_list = ["sessionID","lang","pageName","pageTitle","menu","pageOrder","answer","answerCorrect","lastUpdate"]
    df_quiz = df_read_csv_data(path_quiz, col_list)
    
    # Quiz group by sessionID
    print("> Getting Quiz ratio totals")
    df_quiz_ratio = quiz_correct_ratio(df_quiz, "sessionID")
    print(df_quiz_ratio.head())
    print()

    print("> Saving Quiz ratio totals")
    path_out = Path(stats_dir) / quiz_stats_file
    df_quiz_ratio.to_csv(path_out,sep=";", index=False)

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
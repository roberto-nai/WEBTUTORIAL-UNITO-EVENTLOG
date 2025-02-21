# EDU Event Log
![PyPI - Python Version](https://img.shields.io/badge/python-3.12-3776AB?logo=python)    

This project transforms three tables of a MySQL database (in CSV format) into an event log.

### Python Web Tutorial
Web tutorial from which events were recorded: [https://webtutorial.altervista.org/python/page-01.php](https://webtutorial.altervista.org/python/page-01.php)  

### > Script Execution
```01_survey_clean.py```  
Starting from the raw survey data (```SURVEY_GOOGLE_FILE```), it cleans the columns with the answers and creates two new files ```SURVEY_FILE_CLEAN``` and ```SURVEY_GOOGLE_FILE_CLEAN_MAP```. Save statistics in ```SURVEY_GOOGLE_FILE_STATS```.  
```02_quiz_clean.py```  
Starting from the raw quiz data (```QUIZ_FILE```), it extracts quiz statistics for each sessionID (total quizzes, correct, incorrect, percentage of correct). Save statistics in ```QUIZ_STATS_FILE```.    
```03_csv_to_log.py```  
Starting from the raw events data (```EVENTS_FILE```), it extracts the events for an event log, adding also the quiz and survey data obtained from the previously executed scripts. Saves the event log at page (file with ```_PAGE_```) and paragraph level (file with ```_PARA_```).   
```04_log_enrichment.ipynb```  
Enriches the event log created in the previous step.  
```05_log_correlations.ipynb```  
It performs the Shapiro-Wilk test on the features of interest, then performs Pearson's correlation (for normal distributions) or Spearman's correlation (for non-normal distributions).  
```06_log_survey_remove.ipynb```  
Removes events of type SURVEY from the event log.   
```07_log_complexity.ipynb```  
It calculates the cyclomatic complexity (CC) and the Coefficient of Variation (CV) on the event log.  
 ```08_log_analysis.ipynb```  
Calculate event log statistics.  


### > Script Dependencies
See ```requirements.txt``` for the required libraries (```pip install -r requirements.txt```).  

### > Directories
```config```  
Directory with the configuration file in YAML format (```config.yml```) and script to read it (```config_reader.py```).    
```data```  
Data raw obtained from the database (in CSV format).  
```data_log```    
Event log raw obtained from database (in CSV format) to be filtered in DISCO or ProM; ```*_PAGE_*.csv``` is the event log at the web page level, ```*_PARA_*.csv``` is the event log at the paragraph level of the web page.  
```plots```    
Charts related to statistics.  
```stats```    
Survey, quiz, and event log statistics. 

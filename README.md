# EDU Event Log
![PyPI - Python Version](https://img.shields.io/badge/python-3.12-3776AB?logo=python)    

This project transforms three tables of a database (in CSV format) into an event log.

### Python Web Tutorial
Web tutorial from which events were recorded: [https://webtutorial.altervista.org/python/page-01.php](https://webtutorial.altervista.org/python/page-01.php)  

### > Script Execution
```01_survey_clean.py```
Starting from the raw survey data (```SURVEY_GOOGLE_FILE```), it cleans the columns with the answers and creates a new file with the name defined in SURVEY_FILE_CLEAN.  

### > Script Dependencies
See ```requirements.txt``` for the required libraries (```pip install -r requirements.txt```).  

### > Directories
```config```  
Directory with the configuration file in YAML format (```config.yml```) and script to read it (```config_reader.py```).    
```data```  
Data raw obtained from the database (in CSV format).  
```data_log```    
Event log raw obtained from database (in CSV format) to be filtered in DISCO or ProM; ```edu_event_log_PAGE_raw.csv``` is the event log at the web page level, ```edu_event_log_PARA_raw.csv``` is the event log at the paragraph level of the web page.  
```stats```    
Survey and event log statistics. 

## > Share
If you use it, please cite:    
```
@InProceedings{10.1007/978-3-031-42682-7_48,
author="Nai, Roberto and Sulis, Emilio and Marengo, Elisa and Vinai, Manuela and Capecchi, Sara",
editor="Viberg, Olga and Jivet, Ioana and Mu{\~{n}}oz-Merino, Pedro J. and Perifanou, Maria and Papathoma, Tina",
title="Process Mining on Students' Web Learning Traces: A Case Study with an Ethnographic Analysis",
booktitle="Responsive and Sustainable Educational Futures",
year="2023",
publisher="Springer Nature Switzerland",
address="Cham",
pages="599--604",
isbn="978-3-031-42682-7"
}
```
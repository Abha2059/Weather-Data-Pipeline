import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIRECTORY = PROJECT_ROOT / "logs"                                    #We're storing the path of our log directory in a variable(LOG_DIRECTORY) 

LOG_DIRECTORY.mkdir(
    parents=True,                                               #If parent directories are missing, Python can create them too.
    exist_ok=True                                               #If logs/ already exists, don't give an error
)


LOG_FILE = LOG_DIRECTORY / "pipeline.log"                       #This combines the log directory path with the log file name to create the full path to the log file. EX - logs/pipeline.log


logging.basicConfig(                                            #This tells python how should my logging system behave.
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",         #This controls how the log message looks(2026-09-06 01:25:30 - INFO - Pipeline started)
    handlers=[                                                  #A handler determines where the log message should be sent
        logging.FileHandler(LOG_FILE),                          #Save logging messages inside logs/pipeline.log
        logging.StreamHandler()                                 #Also show the log message in the terminal
    ]
)


logger = logging.getLogger(__name__)                            #This creates our logger object and we can use in other file also with __name__.
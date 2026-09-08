import sys
import pendulum
from pathlib import Path
from datetime import datetime

from airflow import DAG                                                     # DAG is the object that represents our workflow
from airflow.operators.python import PythonOperator



PROJECT_ROOT = Path(__file__).resolve().parent.parent                       # Path(__file__)-This gives the path of the current file (weather_pipeline_dag.py). resolve()- This converts the path to an absolute path. parent.parent- This goes up two levels in the directory structure to reach the project root.
SRC_PATH = PROJECT_ROOT / "src"                                             # Gives- Weather-Data-Pipeline/src/

sys.path.insert(0, str(SRC_PATH))                                           # Tell Python to look inside your src folder when importing your project files
#                                                                           # str(SRC_PATH)- Convert the path into a normal Python string
#                                                                            
#                                                                           #sys.path - It contains the locations where Python searches for modules(import).                                    


from main import run_main                                               # Import the complete ETL pipeline from pipeline.py



with DAG(                                                                   # DAG definition
    dag_id="weather_data_pipeline",                                         # This is the name Airflow will show in its UI
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),            # tells Airflow when the DAG's schedule begins.
    schedule=None,
    catchup=False                                                           # This means Airflow won't try to run the DAG for past dates(old records).
) as dag:

    
    weather_pipeline_task = PythonOperator(                                # Airflow allows to execute python functions, when this task runs and name of task, execute this Python function
        task_id="run_weather_pipeline",
        python_callable=run_main                                             # When Airflow runs this task, execute the run_main() function from pipeline.py
    )
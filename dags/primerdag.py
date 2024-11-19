from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Declarar el DAG
dag = DAG (
    dag_id = 'FirstDag',
    description = 'Este es mi primer DAG',
    schedule_interval = None,
    start_date = datetime.today(),
    catchup = False
)

# Tasks
tsk1 = BashOperator(
    task_id = 'Imprimir_Inicio',
    bash_command = 'echo Iniciando',
    dag = dag
)

tsk2 = BashOperator(
    task_id = 'Esperar',
    bash_command = 'sleep 3',
    dag = dag
)

tsk3 = BashOperator(
    task_id = 'Imprimir_Final',
    bash_command = 'echo Finalizando',
    dag = dag
)

# Ejecucion de tasks
tsk1 >> tsk2 >> tsk3
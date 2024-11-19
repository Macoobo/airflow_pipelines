from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Declarar las funciones a utilizar
def sumar(a: int, b: int):
    suma = a + b
    return suma

def multiplicar(a: int, b: int):
    multiplicacion = a * b
    return multiplicacion


# Declarar el DAG
dag = DAG (
    dag_id = 'SecondDag',
    description = 'Este es mi segundo DAG',
    schedule_interval = None,
    start_date = datetime.today(),
    catchup = False
)

# Tasks
tsk1 = PythonOperator(
    task_id = 'Sumando',
    python_callable= sumar,
    # op_args = [8, 6]
    op_kwargs= {
        'a': 8,
        'b': 6
    },
    dag = dag
)

tsk2 = PythonOperator(
    task_id = 'Multiplicando',
    python_callable= multiplicar,
    # op_args = [8, 6]
    op_kwargs= {
        'a': 8,
        'b': 6
    },
    dag = dag
)

# Ejecucion de tasks
tsk1 >> tsk2
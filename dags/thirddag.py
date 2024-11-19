from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models.taskinstance import TaskInstance

# Declarar las funciones a utilizar
def sumar(a: int, b: int, ti: TaskInstance):
    suma = a + b
    ti.xcom_push(key= 'Suma', value= suma)

def multiplicar(a: int, ti: TaskInstance):
    suma = ti.xcom_pull(key= 'Suma')
    multiplicacion = a * suma
    print(f'La multiplicacion es {multiplicacion}')
    return multiplicacion


# Declarar el DAG
dag = DAG (
    dag_id = 'ThirdDag',
    description = 'Este es mi tercer DAG',
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
        'a': 3,
        'b': 5
    },
    dag = dag
)

tsk2 = PythonOperator(
    task_id = 'Multiplicando',
    python_callable= multiplicar,
    # op_args = [8, 6]
    op_kwargs= {
        'a': 3
    },
    dag = dag
)

# Ejecucion de tasks
tsk1 >> tsk2
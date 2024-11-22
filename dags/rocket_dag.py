import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Instantiate a DAG object; this is the starting point of any workflow.
dag = DAG(
    dag_id="Rocket_dag",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval= '@daily', # At what interval the DAG should run
)

# Apply Bash to download the URL response with curl.
download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'", # Descarga el .json en la ruta ./tmp del contenedor de docker
    dag=dag,
)

# A Python function will parse the response and download all rocket pictures.
def _get_pictures():
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        # leemos el .json y lo guardamos en la variable launches 
        launches = json.load(f)
        # launches["results"] guarda la informacion que estamos buscando ['id', 'url', 'launch_library_id', 'name', 'net', 'window_end', 'window_start', "image", etc]
        # launch["image"] accede a la informacion dentro de la columna "image" que se encontraba dentro de launches["results"]
        image_urls = [launch["image"] for launch in launches["results"]] # 
        # Una vez tenemos las urls en una lista, guardamos una a una en la carpeta destino: /tmp/images/ dentro del contenedor de docker
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

# Call the Python function in the DAG with a PythonOperator.
get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

# Set the order of execution of tasks.
download_launches >> get_pictures >> notify
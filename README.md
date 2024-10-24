1. launch airflow 

cd mle-pr-final
docker build -t my_airflow_image -f docker/airflow/Dockerfile .
cd /home/mle-user/mle_projects/mle-pr-final/docker/airflow
docker compose up airflow-init
docker compose down --volumes --remove-orphans
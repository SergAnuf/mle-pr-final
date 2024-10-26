1. launch airflow 

cd mle-pr-final
docker build -t my_airflow_image -f docker/airflow/Dockerfile .
cd /home/mle-user/mle_projects/mle-pr-final/docker/airflow
docker compose up airflow-init
docker compose down --volumes --remove-orphans




Factors	Regularization	Iterations	k
50	0.05	              30	    5    This setup is balanced with your current parameters and might act as a good control case. Balanced Configuration (Baseline)
30	0.1	20	                        5    Fewer Factors & Higher Reg. (Fewer latent factors and stronger regularization might reduce overfitting and make the model less complex)
70	0.05	50	                    3    More Factors & Moderate Regularization
50	0.01	50	                    7    More Iterations & Lower Regularization
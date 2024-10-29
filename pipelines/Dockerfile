FROM apache/airflow:2.7.3-python3.10 

# Install required system packages
USER root
RUN apt-get update && apt-get install -y libgomp1 && apt-get clean

ENV OPENBLAS_NUM_THREADS=1

# Switch back to the airflow user
USER airflow

# Copy the requirements file
COPY requirements.txt ./tmp/requirements.txt

# Upgrade pip and install Python dependencies
RUN pip install -U pip
RUN pip install -r ./tmp/requirements.txt
RUN pip install python-dotenv

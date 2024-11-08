# Load environment variables from .env file
set -o allexport
source ../.env
set +o allexport

# Confirm values (optional debugging step)
echo "DB_DESTINATION_USER=$DB_DESTINATION_USER"
echo "DB_DESTINATION_HOST=$DB_DESTINATION_HOST"
echo "DB_DESTINATION_NAME=$DB_DESTINATION_NAME"
echo "DB_DESTINATION_PORT=$DB_DESTINATION_PORT"
echo "DB_DESTINATION_PASSWORD=$DB_DESTINATION_PASSWORD"
echo "S3_BUCKET_NAME=$S3_BUCKET_NAME"
echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY"

# Start MLflow server
mlflow server \
  --backend-store-uri postgresql://$DB_DESTINATION_USER:$DB_DESTINATION_PASSWORD@$DB_DESTINATION_HOST:$DB_DESTINATION_PORT/$DB_DESTINATION_NAME \
  --default-artifact-root s3://$S3_BUCKET_NAME \
  --no-serve-artifacts

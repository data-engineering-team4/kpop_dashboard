#!/bin/bash

# 변수 선언
IMAGE_NAME="kpopdocker/kpop-airflow:latest"
COMPOSE_FILE_PATH="/home/ubuntu/kpop_dashboard/docker-compose.yml"

# Docker 이미지를 pull 해옵니다.
echo "Pulling latest image..."
docker pull $IMAGE_NAME

# Docker Compose 서비스를 중지합니다.
echo "Stopping existing services..."
docker-compose -f $COMPOSE_FILE_PATH down

# Docker Compose 서비스를 시작합니다.
echo "Starting services with updated image..."
docker-compose -f $COMPOSE_FILE_PATH --profile flower up -d

echo "Update complete."

# 스크립트 종료
exit 0

#!/bin/zsh
DOCKER_IMAGE_NAME="acai-amd64"
FILE_OUTPUT_LOCAL="${DOCKER_IMAGE_NAME}.tar"
REMOTE_USER="root"
REMOTE_HOST="au1-1.edge.icbix.com"
REMOTE_PATH="/root/Programs/acai/${DOCKER_IMAGE_NAME}.tar"

rm ${FILE_OUTPUT_LOCAL}
docker build --platform linux/amd64 -t ${DOCKER_IMAGE_NAME} .
docker save -o ${FILE_OUTPUT_LOCAL} ${DOCKER_IMAGE_NAME}
rsync -avz --progress ${FILE_OUTPUT_LOCAL} ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}
rm ${FILE_OUTPUT_LOCAL}
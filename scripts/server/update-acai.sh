#!/bin/bash

# Define variables
BASE_NAME="acai-amd64"
CONTAINER_NAME="acai"
TAR_FILE="acai-amd64.tar"
#HOST_DATA_DIR="/root/Programs/acai/configs"
#CONTAINER_DATA_DIR="/acai/configs"
VERSION_TAG="prod-$(date +%Y%m%d%H%M%S)"
IMAGE_NAME="${BASE_NAME}:${VERSION_TAG}"

# Load the new image
echo "Loading image from ${TAR_FILE}..."
docker load -i ${TAR_FILE}

# Tag the most recent image with a versioned tag
LATEST_IMAGE_ID=$(docker images --format "{{.ID}}" | head -n 1)
docker tag ${LATEST_IMAGE_ID} ${IMAGE_NAME}
echo "Tagged image as ${IMAGE_NAME}"

# Stop and rename existing container if present
if docker ps -a --format '{{.Names}}' | grep -Eq "^${CONTAINER_NAME}\$"; then
    echo "Stopping existing container..."
    docker stop ${CONTAINER_NAME}
    echo "Disabling restart policy on old container..."
    docker update --restart=no ${CONTAINER_NAME}
    ARCHIVE_NAME="${CONTAINER_NAME}-archived-$(date +%Y%m%d%H%M%S)"
    echo "Renaming old container to ${ARCHIVE_NAME}..."
    docker rename ${CONTAINER_NAME} ${ARCHIVE_NAME}
fi

# Run new container
echo "Starting new production container..."
docker run -d \
    -p 5006:5006 \
    --name ${CONTAINER_NAME} \
    --restart=always \
    ${IMAGE_NAME}
#    -v ${HOST_DATA_DIR}:${CONTAINER_DATA_DIR} \

echo "Production container ${CONTAINER_NAME} now running with image ${IMAGE_NAME}."

#!/usr/bin/env bash

# Deploy Telegram Scraper Docker Image to Google Cloud Artifact Registry
# This script configures gcloud, tags and pushes the Docker image

set -euo pipefail  # Exit on error, undefined vars, and pipe failures

# Configuration variables
PROJECT_ID="pwcnext-sandbox01"
REGION="me-west1"
REPOSITORY="isa-telegram-scraper"
IMAGE_NAME="telegram-scraper"
TAG="latest"
REGISTRY_URL="${REGION}-docker.pkg.dev"
FULL_IMAGE_NAME="${REGISTRY_URL}/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${TAG}"

echo "Starting deployment of ${IMAGE_NAME} to Google Cloud Artifact Registry..."

# Set the Google Cloud project
echo "Setting Google Cloud project to: ${PROJECT_ID}"
if ! gcloud config set project "${PROJECT_ID}"; then
    echo "❌ Failed to set Google Cloud project"
    exit 1
fi

# Configure Docker authentication for Artifact Registry
echo "Configuring Docker authentication for Artifact Registry..."
if ! gcloud auth configure-docker "${REGISTRY_URL}" --quiet; then
    echo "❌ Failed to configure Docker authentication"
    exit 1
fi

# Check if local image exists
echo "Checking if local image '${IMAGE_NAME}' exists..."
if ! docker image inspect "${IMAGE_NAME}" > /dev/null 2>&1; then
    echo "❌ Local image '${IMAGE_NAME}' not found. Please build the image first."
    echo "Run: docker build -t ${IMAGE_NAME} ."
    exit 1
fi

# Tag the image for Artifact Registry
echo "Tagging image: ${IMAGE_NAME} -> ${FULL_IMAGE_NAME}"
if ! docker tag "${IMAGE_NAME}" "${FULL_IMAGE_NAME}"; then
    echo "❌ Failed to tag image"
    exit 1
fi

# Push the image to Artifact Registry
echo "Pushing image to Artifact Registry: ${FULL_IMAGE_NAME}"
# echo "DEBUGGING: Running command: docker push ${FULL_IMAGE_NAME}"
if ! docker push "${FULL_IMAGE_NAME}"; then
    echo "❌ Failed to push image"
    exit 1
fi

echo "✅ Successfully deployed ${IMAGE_NAME} to Google Cloud Artifact Registry!"
echo "Image location: ${FULL_IMAGE_NAME}"

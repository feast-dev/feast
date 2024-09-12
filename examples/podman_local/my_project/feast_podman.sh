#!/bin/bash

# Set default values (replace these placeholders with real values)
IMAGE_REPOSITORY="feastdev/feature-server"
IMAGE_TAG="latest"
LOCAL_DIR="./feature_repo"
REMOTE_DIR="/feature_repo"
CONTAINER_NAME="registry-feature-server"


# Modes, container names, and their respective ports
MODES=("online" "offline" "registry")
COMMANDS=("serve -h 0.0.0.0" "serve_offline -h 0.0.0.0" "serve_registry")
CONTAINER_NAMES=("online-feature-server" "offline-feature-server" "registry-feature-server")
PORTS=("6566:6566" "8815:8815" "6570:6570")


# Prompt for confirmation to start feature servers
read -p "Do you want to start feature-servers? (y/n): " confirm_all

if [[ $confirm_all == [yY] ]]; then
    # Create and run the containers
    for i in ${!MODES[@]}; do
        CONTAINER_NAME=${CONTAINER_NAMES[$i]}
        COMMAND=${COMMANDS[$i]}
        PORT=${PORTS[$i]}

        echo "Starting $CONTAINER_NAME with port mapping $PORT"

        # Run the Podman container using port mappings and volume mounting
        podman run -d \
            --name $CONTAINER_NAME \
            -p $PORT \
            -v $(pwd)/$LOCAL_DIR:$REMOTE_DIR \
            $IMAGE_REPOSITORY:$IMAGE_TAG \
            feast -c $REMOTE_DIR $COMMAND

        echo "$CONTAINER_NAME is running on port $PORT."
    done
    echo "All feature-servers started."
else
    echo "Feature-server startup was canceled."
fi

# Confirmation to apply 'feast apply' in the remote registry container
read -p "Apply 'feast apply' in the remote registry? (y/n): " confirm_apply

if [[ $confirm_apply == [yY] ]]; then
    # Check if the registry-feature-server container is running
    if podman ps -a --format "{{.Names}}" | grep -q "^registry-feature-server$"; then
        echo "Running 'feast apply' in the registry-feature-server container"

        # Execute the feast apply command in the registry container
        podman exec $CONTAINER_NAME feast -c $REMOTE_DIR apply

        echo "'feast apply' command executed successfully in the remote registry."
    else
        echo "Registry feature server container is not running."
    fi
else
    echo "'feast apply' not performed."
fi

# Prompt to delete containers after the work is done
read -p "Do you want to delete all the created feature-servers? (y/n): " confirm_delete

if [[ $confirm_delete == [yY] ]]; then
    for i in ${!CONTAINER_NAMES[@]}; do
        CONTAINER_NAME=${CONTAINER_NAMES[$i]}

        # Check if the container is running before attempting to stop and delete it
        if podman ps -a --format "{{.Names}}" | grep -q "^$CONTAINER_NAME$"; then
            echo "Stopping and deleting $CONTAINER_NAME..."
            podman stop $CONTAINER_NAME
            podman rm $CONTAINER_NAME
        else
            echo "$CONTAINER_NAME is not running."
        fi
    done
    echo "All feature-servers have been deleted."
else
    echo "Feature-servers were not deleted."
fi

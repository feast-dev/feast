#!/bin/bash
#
# Feast Jupyter Server
# Entrypoint Script
#

# Update permissions to allow notebook user access to - Google SA key 
if [ -f ${GOOGLE_APPLICATION_CREDENTIALS} ]
then
    chown -R $NB_UID /etc/gcloud
fi

# Start Jupyter notebook server as notebook user
start-notebook.sh --NotebookApp.token=''

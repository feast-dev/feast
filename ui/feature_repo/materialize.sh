THREE_YEAR_AGO=$(date -v-3y -u +"%Y-%m-%dT%H:%M:%S")
feast materialize-incremental $THREE_YEAR_AGO
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
feast materialize-incremental $CURRENT_TIME
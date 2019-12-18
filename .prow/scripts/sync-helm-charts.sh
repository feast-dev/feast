#!/usr/bin/env bash

# Script to sync local charts to remote helm repository in Google Cloud Storage
# Copied from: https://github.com/helm/charts/blob/master/test/repo-sync.sh

set -o errexit
set -o nounset
set -o pipefail

log_error() {
    printf '\e[31mERROR: %s\n\e[39m' "$1" >&2
}

# Assume working directory is Feast repository root folder
repo_dir=infra/charts
bucket=gs://feast-charts
repo_url=https://feast-charts.storage.googleapis.com/
sync_dir=/tmp/syncdir
index_dir=/tmp/indexdir 

rm -rf $sync_dir $index_dir

echo "Syncing repo '$repo_dir'..."

mkdir -p "$sync_dir"
if ! gsutil cp "$bucket/index.yaml" "$index_dir/index.yaml"; then
    log_error "Exiting because unable to copy index locally. Not safe to proceed."
    exit 1
fi

exit_code=0

for dir in "$repo_dir"/*; do
    if  helm dep update "$dir" && helm dep build "$dir"; then
        helm package --destination "$sync_dir" "$dir"
    else
        log_error "Problem building dependencies. Skipping packaging of '$dir'."
        exit_code=1
    fi
done

if helm repo index --url "$repo_url" --merge "$index_dir/index.yaml" "$sync_dir"; then
    # Move updated index.yaml to sync folder so we don't push the old one again
    mv -f "$sync_dir/index.yaml" "$index_dir/index.yaml"

    gsutil -m rsync "$sync_dir" "$bucket"

    # Make sure index.yaml is synced last
    gsutil -h "Cache-Control:no-cache,max-age=0" cp "$index_dir/index.yaml" "$bucket"
else
    log_error "Exiting because unable to update index. Not safe to push update."
    exit 1
fi

ls -l "$sync_dir"

exit "$exit_code"

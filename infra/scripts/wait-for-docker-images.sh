#!/usr/bin/env bash
#
# This script will block until both the Feast Serving and Feast Core docker images are available for use for a specific tag.
#

[[ -z "$1" ]] && { echo "Please pass the Git SHA as the first parameter" ; exit 1; }

GIT_SHA=$1

# Set allowed failure count
poll_count=0
maximum_poll_count=150

# Wait for Feast Core to be available on GCR
until docker pull gcr.io/kf-feast/feast-core:"${GIT_SHA}"
do
  # Exit when we have tried enough times
  if [[ "$poll_count" -gt "$maximum_poll_count" ]]; then
       exit 1
  fi

  # Sleep and increment counter on failure
  echo "gcr.io/kf-feast/feast-core:${GIT_SHA} could not be found";
  sleep 5;
  ((poll_count++))
done

# Wait for Feast Serving to be available on GCR
until docker pull gcr.io/kf-feast/feast-serving:"${GIT_SHA}"
do
  # Exit when we have tried enough times
  if [[ "$poll_count" -gt "$maximum_poll_count" ]]; then
       exit 1
  fi

  # Sleep and increment counter on failure
  echo "gcr.io/kf-feast/feast-serving:${GIT_SHA} could not be found";
  sleep 5;
  ((poll_count++))
done

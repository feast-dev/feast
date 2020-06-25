set -e

# This script is deleting Long Running Tests on AKS

# It requires the ID of the Long Running Test
# created by ./start.sh script

LIGHT_GREEN='\033[1;32m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

CRON_JOB_ID=$1

if [ -z "$CRON_JOB_ID" ]
then
  printf "${LIGHT_GREEN}Cron Job ID${NC} must be known to stop the session\n\n"

  printf "Please check running cron jobs by executiong following command\n\n"

  printf "${YELLOW}kubectl get jobs${NC}\n"

  exit 1;
fi

kubectl delete cronjob "long-running-tests-$CRON_JOB_ID"

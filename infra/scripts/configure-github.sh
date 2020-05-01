#!/bin/bash

HELP="
$(basename "$0") [--token] [--repo repo_name] [--owner repo_owner] [--branch branch_name]

commands:
  -h [--help]    show this help text
  -t [--token]   Personal Access Token (PAT), you can generate a PAT on https://github.com/settings/tokens page
  -r [--repo]    name of the repo, format is (owner/repo) or just (repo)
  -o [--owner]   owner of the repo, if you set owner in [--repo] parameter, no need to set it here
  -b [--branch]  name of the branch to protect
"

while [[ "$#" -gt 0 ]]
do
  case $1 in
    -h | --help)
      echo "$HELP"
      exit 0
    ;;
    -t | --token)
      TOKEN=$2
    ;;
    -r | --repo)
      IFS='/'
      read -ra TEMP <<< "$2"
      IFS=' '

      REPO_NAME=${TEMP[0]}
      OWNER_NAME=${TEMP[1]}
    ;;
    -o | --owner)
      OWNER_NAME=$2
    ;;
    -b | --branch)
      BRANCH_NAME=$2
    ;;
  esac
  shift
done

if [ -z "$TOKEN" ]
then
  echo "Get Personal Access Token (PAT) from your GitHub Account and paste it below"
  echo "You can click https://github.com/settings/tokens and create a token"
  read -p "PAT : " TOKEN
fi

if [ -z "$REPO_NAME" ]
then
  echo "Format : {owner}/{repo} or {repo}"
  read -p "Name of the repo : " TEMP

  IFS='/'
  read -ra TEMP <<< "$TEMP"
  IFS=' '

  REPO_NAME=${TEMP[0]}
  OWNER_NAME=${TEMP[1]}
fi

if [ -z "$OWNER_NAME" ]
then
  read -p "Name of the owner : " OWNER_NAME
fi

if [ -z "$BRANCH_NAME" ]
then
  read -p "Name of the branch : " BRANCH_NAME
fi

QUERY_PAYLOAD="{ \"query\": \"query getNodeIdOfRepo { repository(name: \\\"$REPO_NAME\\\", owner: \\\"$OWNER_NAME\\\") { id branchProtectionRules(first: 100) { edges { node { id databaseId pattern } } } } }\" }"

RESPONSE=$(curl -v --request POST --header "Authorization: Bearer $TOKEN" --header "Content-Type: application/json" --data-raw "$QUERY_PAYLOAD" https://api.github.com/graphql)

REPO_ID=$(echo "$RESPONSE" | jq -r '.data.repository.id')

MUTATION_PAYLOAD="{ \"query\": \"mutation setBranchProtectionRule { createBranchProtectionRule(input: {repositoryId: \\\"$REPO_ID\\\", pattern: \\\"$BRANCH_NAME\\\", requiredApprovingReviewCount: 2, requiresApprovingReviews: true}) { branchProtectionRule { id } } }\" }"

RESPONSE=$(curl -v --request POST --header "Authorization: Bearer $TOKEN" --header "Content-Type: application/json" --data-raw "$MUTATION_PAYLOAD" https://api.github.com/graphql)

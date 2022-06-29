VERSION_REGEX='[0-9]+\.[0-9]+\.[0-9]+'
OUTPUT_REGEX='^Feast SDK Version: "$VERSION_REGEX"$'
VERSION_OUTPUT=$(feast version)
VERSION=$(echo $VERSION_OUTPUT | grep -oE "$VERSION_REGEX")
VERSION_WITHOUT_PREFIX='0.19.3'
OUTPUT=$(echo $VERSION_OUTPUT | grep -E "$REGEX")
if  [ -n "$OUTPUT" ] &&[ "$VERSION" = "$VERSION_WITHOUT_PREFIX" ]; then  
  echo "Correct Feast Version Installed"
else
  echo "$VERSION_OUTPUT is not in the correct format"
  exit 1
fi

#!/usr/bin/env bash
REGEX='^Feast SDK Version: "feast [0-9]+\.[0-9]+\.[0-9]"$'
OUT=$(feast version)
echo $OUT
if echo $OUT | grep -E "$REGEX" &>/dev/null ; then
  echo "gay"
else 
  echo "not gay"
fi


echo 'Feast SDK Version: "feast 0.21.3"' | grep -e "$REGEX"

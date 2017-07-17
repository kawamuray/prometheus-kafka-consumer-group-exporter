#!/bin/bash

EXITCODE=0

for F in *.go
do
  gofmt -d "$F" | read -r
  if [ $? -eq 0 ]
  then
    echo "Found incorrectly formatted go code:"
    gofmt -d "$F"
    EXITCODE=1
  fi
done

if [ $EXITCODE -eq 0 ]
then
  echo 'All source code files were properly formatted using "gofmt".'
else
  echo 'Not all source code files are formatted using "gofmt".'
fi

exit $EXITCODE

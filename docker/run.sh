#!/usr/bin/env bash

echo "$@"

java -jar build/libs/oneclient-1.0-all.jar "$@"

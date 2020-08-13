#!/bin/sh
set -e

CGO_ENABLED=0 go build -o exec main.go
zip exec.zip exec

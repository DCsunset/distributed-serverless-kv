# openwhisk-grpc

## Introduction

This demo includes a simple key-value store and a word counting action
that simulates MapReduce workflow.

The action will count all the words stored in keys within the range `[0, 20)`,

**Note**:
The number of action invocations are limited to 60 per minute by default.
The default limitations can be changed in helm configuration file when deploying.


## Run this project

(The following steps are tested on Linux.
Some steps might differ on other platforms.)

First, clone this repository directly to `$GOPATH/src/github.com/DCsunset/openwhisk-grpc`
or clone it elsewhere and create a symlink in it.

Then, start the db server in directory `server`:

```
go run *.go
```

Next, mock data in the db in direction `mock`:

```
go run mock.go
```

Then, build the binary in directory `action` (must be statically linked):

```
CGO_ENABLED=0 go build -o exec <action>.go
zip exec.zip exec
```

Then, deploy OpenWhisk (for example, k8s in docker).

Finally, run `action/createAction.sh` to create the action.

To invoke the action, run `action/invokeAction.sh`.

## Generate grpc code from proto

```
protoc -I db --go_out=plugins=grpc:db db/db.proto
```

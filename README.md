# openwhisk-grpc

## Introduction

This demo includes a simple key-value store and a word counting action.

The word counting action can be run in either sequential mode
or parallel mode.

The action accepts the following parameters:

```json
{
	"low": 0, // range [0, high)
	"high": 30, // range (low, 1024]
	"parallel": false
}
```

The action will count all the words stored in keys,
where keys are in the range `[low, high)`.

To invoke parallel actions,
the `parellel` parameter should be changed to `true`
in the file `invokeAction.sh`.

**Note**:
The number of action invocations are limited to 60 per minute by default,
which means `high-low` should be at least smaller than 60 by default.
The default limitations can be changed in helm configuration file when deploying.


## Run this project

(The following steps are tested on Linux.
Some steps might differ on other platforms.)

First, clone this repository directly to `$GOPATH/src/github/DCsunset/openwhisk-grpc`
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
CGO_ENABLED=0 go build -o exec action.go
zip exec.zip exec
```

Then, deploy OpenWhisk (for example, k8s in docker).

Finally, run `action/createAction.sh` to create the action.

To invoke the action, run `action/invokeAction.sh`.

## Generate grpc code from proto

```
protoc -I db --go_out=plugins=grpc:db db/db.proto
```

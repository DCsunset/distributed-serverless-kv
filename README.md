# openwhisk-grpc

## Run this project

(The following steps are tested on Linux.
Some steps might differ on other platforms.)

First, clone this repository directly to `$GOPATH/src/github/DCsunset/openwhisk-grpc`
or clone it elsewhere and create a symlink in it.

Then, start the db server in directory `server`:

```
go run *.go
```

Next, build the binary in directory `action` (must be statically linked):

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

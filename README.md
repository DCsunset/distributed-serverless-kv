# distributed-serverless-kv

## Introduction

This demo is a simple in-memory distributed key-value database.

**Note**:
The number of action invocations in OpenWhisk are limited to 60 per minute by default.
The default limitations can be changed in helm configuration file when deploying it.


## Run this project

(The following steps are tested on Linux.
Some steps might differ on other platforms.)

First, clone this repository directly to `$GOPATH/src/github.com/DCsunset/openwhisk-grpc`
or clone it elsewhere and create a symlink in it.

Next, modify the `server.json` in the `server` directory.
Since this prototype is elastic,
the `initial` field means the first server to store the data.
The `availableServers` field means the servers available to be used later.
The `self` field is the address of the current machine.
The `servers` field shows all the servers used for the db.
The `threshold` field means when the key-value pairs reach the threshold,
data should be split and sent to other available servers.

Then, start the db server in directory `server` on every machine:

```
go run *.go
```

Next, deploy OpenWhisk (for example, k8s in docker).

Finally, run `createAction.sh` in the `demo` directory to create some demo actions.

To invoke the action, change the parameters in `invokeAction.sh` and execute it.

## Generate grpc code from proto

```
protoc -I db --go_out=plugins=grpc:db db/db.proto
```

## Benchmarks

### Steps

To run the benchmarking, first deploy the simple db and the distributed db.

Then modify the `createAction.sh` script and run it to create the benchmark action.

Finally, run the `invokeAction.sh` script to start the benchmarking.

### Results

(10ms delay has been added intentionally to read and write operations)

| Operations       | Simple Database | Distributed Database |
| ---------------- | --------------- | -------------------- |
| Read Latency     | 25 ms           | 20 ms                |
| Write Latency    | 50 ms           | 35 ms                |
| Read Throughput  | 24 GB/s         | 16 GB/s              |
| Write Throughput | 7 GB/s          | 11 GB/s              |

#!/bin/sh
set -e
export APIHOST=aqua02:31001
export AUTH="23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"
export ACTION=benchmark

CGO_ENABLED=0 go build -o exec benchmark.go
zip exec.zip exec

echo "{\"namespace\": \"guest\", \"actionName\": \"$ACTION\", \"exec\": { \"kind\": \"blackbox\", \"image\": \"openwhisk/dockerskeleton\", \"binary\": true, \"code\": \"$(base64 exec.zip)\"} }" | \
curl --insecure -X PUT -u $AUTH -H "Content-Type: application/json" -d @- https://$APIHOST/api/v1/namespaces/guest/actions/$ACTION?overwrite=true > /dev/null

rm exec exec.zip

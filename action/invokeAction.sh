#!/bin/sh
export APIHOST=172.19.0.2:31001
export AUTH="23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"
export ACTION=test-grpc

curl --insecure -X POST -u $AUTH -H "Content-Type: application/json" "https://$APIHOST/api/v1/namespaces/guest/actions/$ACTION?blocking=true&result=true"

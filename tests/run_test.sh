#!/bin/bash

set -e
set -x
export NIMBUSIO_CONNECTION_TIMEOUT=360.0
export NIMBUS_IO_SERVICE_PORT="9000"
export NIMBUS_IO_SERVICE_HOST="dev.nimbus.io"
export NIMBUS_IO_SERVICE_DOMAIN="dev.nimbus.io"
export NIMBUS_IO_SERVICE_SSL="0"
export PYTHONPATH="${HOME}/twisted_client_for_nimbusio"

PYTHON="python2.7"

"${PYTHON}" "${HOME}/twisted_client_for_nimbusio/tests/test_client.py" $@

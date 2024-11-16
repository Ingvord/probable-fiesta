#1/bin/bash

docker run -it \
-p 6650:6650 \
-p 8080:8080 \
--mount source=pulsardata,target=/pulsar/data \
--mount source=pulsarconf,target=/pulsar/conf \
--name pulsar-standalone \
apachepulsar/pulsar:4.0.0 \
bin/pulsar standalone
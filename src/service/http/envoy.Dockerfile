# starting from v1.11.0, cors of allow-origin star will not be supported
# TODO(cao): need to figure out a way how to replace it. So not using latest tag here.
FROM envoyproxy/envoy:v1.12.1

COPY ./http/envoy.yaml /etc/envoy/envoy.yaml

# config logging in envoy.yaml file
# or pass options to the cmd "-l info --log-path /tmp/envoy_info.log"
CMD /usr/local/bin/envoy -c /etc/envoy/envoy.yaml

# starting from v1.11.0, cors of allow-origin star will not be supported
# TODO(cao): need to figure out a way how to replace it. So not using latest tag here.
FROM envoyproxy/envoy:v1.10.0

COPY ./http/envoy.yaml /etc/envoy/envoy.yaml
CMD /usr/local/bin/envoy -c /etc/envoy/envoy.yaml -l trace --log-path /tmp/envoy_info.log
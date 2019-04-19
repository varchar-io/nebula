
FROM ubuntu:14.04

EXPOSE 9090
COPY ./greeter_server /etc/nebula/greeter_server
CMD ["/etc/nebula/greeter_server"]
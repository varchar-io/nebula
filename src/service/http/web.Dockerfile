FROM python:3.6.8-alpine3.8

COPY http/nebula /etc/web/nebula

EXPOSE 8088

WORKDIR /etc/web/nebula

# run the simple web server
CMD python3 -m http.server 8088
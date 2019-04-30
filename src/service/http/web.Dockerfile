FROM python3.5:latest

COPY nebula /etc/web/nebula

EXPOSE 8088

WORKDIR /etc/web/nebula

CMD ["python3 -m http.server 8088"]
FROM node:14-alpine

COPY http/nebula /etc/web/nebula

WORKDIR /etc/web/nebula

# install node packages
# RUN chmod -R 777 /etc/web/nebula
# RUN apk add --no-cache make gcc g++ linux-headers python
# RUN npm config set package-lock false
# RUN npm install --save --production
# A way to support native addon build as well after clean to keep image size small
RUN apk add --no-cache --virtual .build-deps alpine-sdk python3 \
    && npm install --production --silent \
    && apk del .build-deps

# run the simple web server
ARG NODE_PORT=8088
ENV NODE_PORT=${NODE_PORT}

# docker-compose DNS addressing
ARG NS_ADDR=server:9190
ENV NS_ADDR=${NS_ADDR}

ARG SERVER

# replace the server endpoint 
# RUN sed -i "s|{SERVER-ADDRESS}|${SERVER}|g" /etc/web/nebula/web.min.js

EXPOSE ${NODE_PORT}
CMD ["node", "node.js"]
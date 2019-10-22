FROM node:12-alpine

COPY http/nebula /etc/web/nebula

WORKDIR /etc/web/nebula

# install node
# RUN chmod -R 777 /etc/web/nebula
# RUN apk --no-cache add g++ gcc libgcc libstdc++ linux-headers make python
RUN npm config set package-lock false
RUN npm install --save --production

# why do we need to switch user? Just use root
# USER prod

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
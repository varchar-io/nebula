FROM node:12-alpine

COPY http/nebula /etc/web/nebula

WORKDIR /etc/web/nebula

# install node
# RUN chmod -R 777 /etc/web/nebula
# RUN apk --no-cache add g++ gcc libgcc libstdc++ linux-headers make python
RUN npm install --save --production
USER node

# run the simple web server
ARG NODE_PORT=8088
ENV NODE_PORT=${NODE_PORT}

# docker-compose DNS addressing
ARG NS_ADDR=server:9190
ENV NS_ADDR=${NS_ADDR}

EXPOSE ${NODE_PORT}
CMD ["node", "node.js"]
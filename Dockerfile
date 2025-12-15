FROM node:24-alpine

RUN apk upgrade && apk add --no-cache tini && rm -rf /var/cache/pkg/*
WORKDIR "/app"
COPY ./package.json ./package-lock.json ./
RUN npm install \
  && npm cache clean --force \
  && rm -r /tmp/*
COPY ./src/ ./src/
EXPOSE 3000
RUN chown -R node: /app
USER node

ENTRYPOINT ["tini", "--"]
CMD ["node", "src/server.js"]

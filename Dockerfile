FROM node:lts-alpine

WORKDIR /usr/src/app

RUN apk update && apk add --upgrade transmission-remote

COPY ["package.json", "package-lock.json*", "./"]

RUN npm install --silent && mv node_modules ../

COPY . .

EXPOSE 3000

RUN chown -R node /usr/src/app

USER node

CMD npm run torrent

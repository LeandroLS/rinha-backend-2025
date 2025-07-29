FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY index.ts ./

RUN npm run build
RUN npm prune

EXPOSE 3000

CMD ["npm", "start"]
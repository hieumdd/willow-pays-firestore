FROM node:18-bullseye-slim as builder

ENV NODE_ENV build

WORKDIR /app

COPY ./package*.json .

RUN npm ci

COPY . .

RUN npx tsc --project tsconfig.build.json

RUN npm prune --production

# ---

FROM node:18-bullseye-slim

ENV NODE_ENV production

WORKDIR /app

COPY --from=builder /app/package*.json ./
COPY --from=builder /app/node_modules/ ./node_modules/
COPY --from=builder /app/dist/ ./dist/

CMD ["node", "dist/index.js"]

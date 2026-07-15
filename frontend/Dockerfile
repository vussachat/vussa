FROM node:22-bookworm-slim AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:22-bookworm-slim
WORKDIR /app
ENV HOST=0.0.0.0
ENV PORT=3000
COPY --from=builder /app/build ./build
USER node
EXPOSE 3000
CMD ["node", "build"]

# Build a PocketBase binary with pbreplication baked in.
# Build context must be the repository root:
#   docker build -f example/Dockerfile -t pbreplication-demo .

FROM golang:1.24-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /pb ./example

# busybox wget (bundled with alpine) is enough for the healthcheck;
# add "RUN apk add --no-cache ca-certificates" if your nodes talk to
# each other over public HTTPS.
FROM alpine:3.20
COPY --from=build /pb /usr/local/bin/pb

VOLUME /pb_data
EXPOSE 8090

HEALTHCHECK --interval=5s --timeout=3s --retries=20 \
  CMD wget -qO- http://127.0.0.1:8090/api/health || exit 1

CMD ["pb", "serve", "--http=0.0.0.0:8090", "--dir=/pb_data"]

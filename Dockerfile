FROM golang:1.19-alpine3.16 AS build

RUN apk add gcc musl-dev

WORKDIR /go/src/graph-vulcan-assets

COPY . .
RUN go build -tags musl ./cmd/graph-vulcan-assets


FROM alpine:3.16

WORKDIR /app

COPY --from=build /go/src/graph-vulcan-assets/graph-vulcan-assets graph-vulcan-assets

ENTRYPOINT ["/app/graph-vulcan-assets"]

# build
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY tcptee.go .
RUN go build -ldflags="-s -w" -o /out/tcptee tcptee.go

# run
FROM alpine:3.20
RUN adduser -D -H -u 10001 app
COPY --from=build /out/tcptee /usr/local/bin/tcptee
USER app
ENTRYPOINT ["/usr/local/bin/tcptee"]

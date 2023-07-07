FROM golang:1.19-alpine 

WORKDIR $GOPATH

RUN go install github.com/anjor/go-fil-dataprep/cmd/data-prep@277ca0e7f83bb3ad3cd05d9e62e5d140fc409a51

RUN mkdir /app
WORKDIR /app

ENTRYPOINT ["data-prep"]


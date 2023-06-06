GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOINSTALL=$(GOCMD) install

# These are the values we want to pass for VERSION and BUILD
# git tag 1.0.1
# git commit -am "One more change after the tags"
VERSION=`git describe --tags`
BRANCH=`git rev-parse --abbrev-ref HEAD`
REVISION=`git rev-parse HEAD`
BUILD=`date +%FT%T%z`

LDFLAGS=-ldflags "-w -s -X main.Version=${VERSION} -X main.Build=${BUILD} -X main.Branch=${BRANCH} -X main.Revision=${REVISION}"
BINDIR=.

all: build install

build:
	cd cmd/data-prep; \
	$(GOBUILD) $(LDFLAGS)

install:
	cd cmd/data-prep; \
	$(GOINSTALL) $(LDFLAGS)

clean:
	if [ -f ./cmd/data-prep/data-prep ] ; then rm ./cmd/data-prep/data-prep ; fi

.PHONY: clean install


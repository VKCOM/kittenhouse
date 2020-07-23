GO = go

NOW = $(shell date +%Y%m%d%H%M%S)
OS = $(shell uname -n -m)
AFTER_COMMIT = $(shell git log --format="%H" -n 1)

LDFLAGS = "-X 'main.BuildTime=$(NOW)' -X 'main.BuildOSUname=$(OS)' -X 'main.BuildCommit=$(AFTER_COMMIT)' -extldflags '-O2'"

FLAGS = -tags netgo
EXTRA_DEPS =

BIN = $(CURDIR)/target/kittenhouse
SRC = *.go */*.go

.PHONY: all clean

$(BIN): $(SRC) $(EXTRA_DEPS)
	$(GO) build $(FLAGS) -ldflags $(LDFLAGS) -o $(BIN)

all: $(BIN)

clean:
	rm -f $(BIN)
	rm -f $(EXTRA_DEPS)

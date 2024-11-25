# Define the target binary name
BINARY_NAME=oem-converter

# Define the source files and proto files
SRCS := $(wildcard *.go)

# Define flags add compilation data/time
FLAGS := -ldflags "-X main.CompileDate=`date -u +.%Y%m%d.%H%M%S`"

# Detect the current platform (OS)
UNAME_S := $(shell uname -s)
# Detect the Architecture (Hardware)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_S),Linux)
	CURRENT_PLATFORM = linux
	ifeq  ($(UNAME_M),aarch64)
		CURRENT_ARCH = arm64
		TARGET = build-linux-arm
	else
		CURRENT_ARCH = x86
		TARGET = build-linux
	endif
else ifeq ($(UNAME_S),Darwin)
	CURRENT_PLATFORM = macos
    ifeq ($(UNAME_M),i386)
    	CURRENT_ARCH = x86
    	TARGET = build-macos-x86
    else ifeq ($(UNAME_M),amd64)
    	CURRENT_ARCH = amd64
    	TARGET = build-macos
    else ifeq ($(UNAME_M),arm64)
    	CURRENT_ARCH = arm64
    	TARGET = build-arm64
    endif
else
	@echo "current platform: unknown"
	CURRENT_PLATFORM = unknown
endif

$(info $$CURRENT_PLATFORM = $(CURRENT_PLATFORM))
$(info $$CURRENT_ARCH = $(CURRENT_ARCH))

$(info $$TARGET = $(TARGET))

# Define the default target
default: $(TARGET)

build:
	@echo "Building for..."
	GOARCH=amd64 GOOS=linux go build $(FLAGS) -o ${BINARY_NAME}-linux $(SRCS)

build-linux:
	@echo "Building for Linux..."
	GOOS=linux GOARCH=amd64 go build $(FLAGS) -o $(BINARY_NAME)-linux-$(CURRENT_ARCH) $(SRCS)

build-linux-arm:
	@echo "Building for Linux arm..."
	GOOS=linux GOARCH=arm64 go build $(FLAGS) -o $(BINARY_NAME)-linux-$(CURRENT_ARCH) $(SRCS)

build-macos:
	@echo "Building for macOS..."
	GOOS=darwin GOARCH=amd64 go build $(FLAGS) -o $(BINARY_NAME)-darwin-$(CURRENT_ARCH) $(SRCS)

build-arm64:
	@echo "Building for macOS arm..."
	GOOS=darwin GOARCH=arm64 go build $(FLAGS) -o $(BINARY_NAME)-darwin-$(CURRENT_ARCH) $(SRCS)

build-macos-x86:
	@echo "Building for macOS x86..."
	GOOS=darwin GOARCH=386 go build $(FLAGS) -o $(BINARY_NAME)-darwin-$(CURRENT_ARCH) $(SRCS)

run: build
	./${BINARY_NAME}

test:
	go test -v ./...

clean:
	@go clean
	rm ${BINARY_NAME}-darwin
	rm ${BINARY_NAME}-linux

.PHONY: build proto clean test run
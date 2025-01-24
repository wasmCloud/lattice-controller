SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules
MAKEFLAGS += --no-print-directory
MAKEFLAGS += -S

OS_NAME := $(shell uname -s | tr '[:upper:]' '[:lower:]')
ifeq ($(OS_NAME),darwin)
	NC_FLAGS := -czt
else
	NC_FLAGS := -Czt
endif

.DEFAULT: help

CARGO ?= cargo
CARGO_CLIPPY ?= cargo-clippy
DOCKER ?= docker
NATS ?= nats

# Defaulting to the local registry since multi-arch containers have to be pushed
WADM_TAG ?= localhost:5000/wasmcloud/wadm:latest
# These should be either built locally or downloaded from a release.
BIN_AMD64 ?= wadm-amd64
BIN_ARM64 ?= wadm-aarch64

help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_\-.*]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

all: build test

###########
# Tooling #
###########

# Ensure that clippy is installed
check-cargo-clippy:
ifeq ("",$(shell command -v $(CARGO_CLIPPY)))
	$(error "ERROR: clippy is not installed (see: https://doc.rust-lang.org/clippy/installation.html)")
endif

#########
# Build #
#########

lint: check-cargo-clippy ## Run code lint
	$(CARGO) fmt --all --check
	$(CARGO) clippy --all-features --all-targets --workspace

build: ## Build wadm
	$(CARGO) build --bin wadm

build-docker: ## Build wadm docker image
    ifndef BIN_AMD64
        $(error BIN_AMD64 is not set, required for docker building)
    endif
    ifndef BIN_ARM64
        $(error BIN_ARM64 is not set, required for docker building)
    endif

	$(DOCKER) buildx build --platform linux/amd64,linux/arm64 \
		--build-arg BIN_AMD64=$(BIN_AMD64) \
		--build-arg BIN_ARM64=$(BIN_ARM64) \
		-t $(WADM_TAG) \
		--push .


########
# Test #
########

# An optional specific test for cargo to target

CARGO_TEST_TARGET ?=

test:: ## Run tests
ifeq ($(shell nc $(NC_FLAGS) -w1 127.0.0.1 4222 || echo fail),fail)
	$(DOCKER) run --rm -d --name wadm-test -p 127.0.0.1:4222:4222 nats:2.10.18-alpine -js
	$(CARGO) test $(CARGO_TEST_TARGET) -- --nocapture
	$(DOCKER) stop wadm-test
else
	$(CARGO) test $(CARGO_TEST_TARGET) -- --nocapture
endif

test-e2e:: ## Run e2e tests
ifeq ($(shell nc $(NC_FLAGS) -w1 127.0.0.1 4222 || echo fail),fail)
	@$(MAKE) build
	@# Reenable this once we've enabled all tests
	@# RUST_BACKTRACE=1 $(CARGO) test --test e2e_multitenant --features _e2e_tests --  --nocapture 
	RUST_BACKTRACE=1 $(CARGO) test --test e2e_multiple_hosts --features _e2e_tests --  --nocapture 
	RUST_BACKTRACE=1 $(CARGO) test --test e2e_upgrades --features _e2e_tests --  --nocapture 
else
	@echo "WARN: Not running e2e tests. NATS must not be currently running"
	exit 1
endif

test-individual-e2e:: ## Runs an individual e2e test based on the WADM_E2E_TEST env var
ifeq ($(shell nc $(NC_FLAGS) -w1 127.0.0.1 4222 || echo fail),fail)
	@$(MAKE) build
	RUST_BACKTRACE=1 $(CARGO) test --test $(WADM_E2E_TEST) --features _e2e_tests --  --nocapture 
else
	@echo "WARN: Not running e2e tests. NATS must not be currently running"
	exit 1
endif

###########
# Cleanup #
###########

stream-cleanup: ## Removes all streams that wadm creates
	-$(NATS) stream del wadm_commands --force
	-$(NATS) stream del wadm_events --force
	-$(NATS) stream del wadm_event_consumer --force
	-$(NATS) stream del wadm_notify --force
	-$(NATS) stream del wadm_status --force
	-$(NATS) stream del KV_wadm_state --force
	-$(NATS) stream del KV_wadm_manifests --force

.PHONY: check-cargo-clippy lint build build-watch test stream-cleanup clean test-e2e test 

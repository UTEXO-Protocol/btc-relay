# Shared project defaults for Makefile.

# Base variables.
ENVIRONMENT    ?=
IMAGE_TAG      ?= latest
REGISTRY_HOST  ?= ghcr.io/utexo-protocol
CONTAINER_NAME ?= btc-relayer
ENV_FILE       ?= .env
ARTIFACTS_DIR  ?= $(CURDIR)/artifacts
PORT           ?= 9090

CURRENT_DATE_TIME := $(shell date +'%Y-%m-%d')
LATEST_COMMIT     := $(shell git rev-parse --short HEAD)

# Image names.
UTEXO_BTC_RELAY_IMAGE ?= btc-relayer

# Variables for build — BTC Relay.
IMAGE_UTEXO_BTC_RELAY_BACKUP = $(REGISTRY_HOST)/$(UTEXO_BTC_RELAY_IMAGE)$(ENVIRONMENT):$(CURRENT_DATE_TIME)-$(LATEST_COMMIT)
IMAGE_UTEXO_BTC_RELAY_LATEST = $(REGISTRY_HOST)/$(UTEXO_BTC_RELAY_IMAGE)$(ENVIRONMENT):$(IMAGE_TAG)

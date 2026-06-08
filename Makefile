# Project config (tracked in repo).
include config.mk

export DOCKER_BUILDKIT=1

.PHONY: build tag push docker run stop logs clean-image help

build: ## Build Docker image once (latest tag).
	docker build -f ./Dockerfile -t $(IMAGE_UTEXO_BTC_RELAY_LATEST) .

tag: ## Tag latest image with backup tag.
	docker tag $(IMAGE_UTEXO_BTC_RELAY_LATEST) $(IMAGE_UTEXO_BTC_RELAY_BACKUP)

push: ## Push latest + backup tags.
	docker push $(IMAGE_UTEXO_BTC_RELAY_LATEST)
	docker push $(IMAGE_UTEXO_BTC_RELAY_BACKUP)

docker: build tag push ## Build, tag and push image.

run: ## Run relayer container with .env and persisted state.
	mkdir -p $(ARTIFACTS_DIR)
	docker run -d \
		--name $(CONTAINER_NAME) \
		--restart unless-stopped \
		--env-file $(ENV_FILE) \
		-p $(PORT):9090 \
		-v $(ARTIFACTS_DIR):/app/artifacts \
		$(IMAGE_UTEXO_BTC_RELAY_LATEST)

stop: ## Stop and remove relayer container.
	-docker stop $(CONTAINER_NAME)
	-docker rm $(CONTAINER_NAME)

logs: ## Tail relayer container logs.
	docker logs -f $(CONTAINER_NAME)

clean-image: ## Remove local image tags.
	-docker rmi $(IMAGE_UTEXO_BTC_RELAY_LATEST)
	-docker rmi $(IMAGE_UTEXO_BTC_RELAY_BACKUP)

help: ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

OM_INGESTION_IMAGE_VERSION := 1.4.7

.PHONY: help
help:  ## shows the Makefile targets with information
ifeq ($(OS),Windows_NT)
	$(info ***** This target is not supported for windows *****)
else
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) |  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
endif

.PHONY: local-openmetadata-stack
local-openmetadata-stack: export OPENMETADATA_INGESTION_IMAGE_VERSION=$(OM_INGESTION_IMAGE_VERSION)
local-openmetadata-stack:  ## first shutdowsn any existing and then starts a local openmetadata stack for testing
	docker compose -f ./local-openmetadata-stack/docker-compose.yml down -v && docker compose -f ./local-openmetadata-stack/docker-compose.yml up -d

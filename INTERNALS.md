# Internals

## Files Used When the Pipeline Runs

### `make build` — image construction only
- `docker-compose.yaml` — which Dockerfiles to build
- `Dockerfile.spark` — Spark image (downloads JARs)
- `producer/Dockerfile` + `producer/requirements.txt` — producer image

### `make up` — container startup
- `docker-compose.yaml` — service definitions, port mappings, volumes
- `.env` — credentials injected into producer, spark-master, spark-worker

### `make stream` — active streaming
- `Makefile` — the `stream` target command
- `spark/jobs/stream_prices.py` — the Spark job (bind-mounted into the container, not baked into the image)

### Producer (running continuously inside its container)
- `producer/producer.py` — baked into the image at build time

---

### Never used at runtime
- `terraform/` — only for initial Snowflake provisioning
- `producer/Dockerfile`, `producer/requirements.txt`, `Dockerfile.spark` — build time only
- `.env.example`, `terraform.tfvars.example` — reference only
- `README.md`, `docs/`, `.gitignore` — repo docs only

The one interesting case is `stream_prices.py` — it's bind-mounted (`./spark/jobs:/opt/spark-jobs`), so edits take effect immediately on the next `make stream` without rebuilding the image.

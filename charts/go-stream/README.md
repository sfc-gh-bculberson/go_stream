go-stream Helm Chart
====================

This chart deploys the `go_stream` container and loads environment variables from a `.env_no_exports` file at chart root by default.

Quickstart
```bash
helm upgrade --install go-stream ./charts/go-stream
```

Override dotenv file path (pack it with the chart):
```bash
helm upgrade --install go-stream ./charts/go-stream \
  -f charts/go-stream/values.yaml \
  --set envFromDotenv=./charts/go-stream/.env_no_exports
```

Values
- image.repository: container repo
- image.tag: tag
- container.eps, container.batch, container.count, container.buffer
- envFromDotenv: path to dotenv file bundled with the chart (default `.env_no_exports`)

Dotenv parsing
- Lines like `KEY=VALUE` are mapped into a ConfigMap
- Only `JWT` is mapped into a Secret



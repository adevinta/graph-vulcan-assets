# Security Graph - Vulcan Assets

## Security Graph

Security Graph is a data architecture that provides real-time views of assets
and their relationships. Assets to be considered include software, cloud
resources and, in general, any piece of information that a security team needs
to protect.

Security Graph not only stores the catalog of active assets and the assets
assigned to teams. It also keeps a historical log of the multidimensional
relationships among these assets, including their attributes relevant to
security.

## Vulcan Assets Consumer

The Vulcan asset consumer is a long running job that allows to feed the assets
in the [Vulcan API] into the [Graph Asset Inventory] by consuming the [Vulcan
assets stream].

## Test

Execute the tests:

```
_script/test -cover ./...
```

`_script/test` makes sure the testing infrastructure is up and running and then
runs `go test` with the provided arguments. It also disables test caching and
avoids running multiple test programs in parallel.

Stop the testing infrastructure:

```
_script/clean
```

## Environment Variables

The following environment variables are the **required**:

| Variable | Description | Example |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `kafka.example.com:9092` |
| `INVENTORY_ENDPOINT` | Endpoint of the Security Graph Asset Inventory | `https://inventory.example.com` |
| `AWS_ACCOUNT_ANNOTATION_KEY` | Key of the annotation that contains the asset's parent AWS account | `discovery/aws/account` |

The following environment variables are **optional**:

| Variable | Description | Default |
| --- | --- | --- |
| `LOG_LEVEL` | Log level. Valid values: `info`, `debug`, `error`, `disabled` | `info` |
| `RETRY_DURATION` | Time between retries if the stream processor fails. If the value is `0` the command exits on error | `5s` |
| `KAFKA_GROUP_ID` | Kafka consumer group ID | `graph-vulcan-assets` |
| `KAFKA_USERNAME` | Kafka username | |
| `KAFKA_PASSWORD` | kafka password | |
| `INVENTORY_INSECURE_SKIP_VERIFY` | If the value is `1` then skip TLS verification | `0` |

If both `KAFKA_USERNAME` and `KAFKA_PASSWORD` are not specified, plaintext
un-authenticated mode is used.

The directory `_env` in this repository contains some example configurations.

## Contributing

**This project is in an early stage, we are not accepting external
contributions yet.**

To contribute, please read the contribution guidelines in [CONTRIBUTING.md].


[Vulcan API]: https://github.com/adevinta/vulcan-api
[Graph Asset Inventory]: https://github.com/adevinta/graph-asset-inventory-api
[Vulcan assets stream]: https://github.com/adevinta/vulcan-api/blob/master/docs/asyncapi.yaml
[CONTRIBUTING.md]: CONTRIBUTING.md

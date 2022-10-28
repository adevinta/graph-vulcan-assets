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


[Vulcan API]: https://github.com/adevinta/vulcan-api
[Graph Asset Inventory]: https://github.com/adevinta/graph-asset-inventory-api
[Vulcan assets stream]: https://github.com/adevinta/vulcan-api/blob/master/docs/asyncapi.yaml

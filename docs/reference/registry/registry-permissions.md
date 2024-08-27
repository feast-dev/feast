# Registry Permissions and Access Control


## API Endpoints and Permissions

| Endpoint                 | Resource Type       | Permission             | Description                                                    |
| ------------------------ |---------------------|------------------------| -------------------------------------------------------------- |
| ApplyEntity              | Entity              | Create, Update, Delete | Apply an entity to the registry                                 |
| GetEntity                | Entity              | Read                   | Get an entity from the registry                 |
| ListEntities             | Entity              | Read                   | List entities in the registry                   |
| DeleteEntity             | Entity              | Delete                 | Delete an entity from the registry              |
| ApplyDataSource          | DataSource          | Create, Update, Delete | Apply a data source to the registry             |
| GetDataSource            | DataSource          | Read                   | Get a data source from the registry             |
| ListDataSources          | DataSource          | Read                   | List data sources in the registry               |
| DeleteDataSource         | DataSource          | Delete                 | Delete a data source from the registry          |
| ApplyFeatureView         | FeatureView         | Create, Update, Delete | Apply a feature view to the registry            |
| GetFeatureView           | FeatureView         | Read                   | Get a feature view from the registry            |
| ListFeatureViews         | FeatureView         | Read                   | List feature views in the registry              |
| DeleteFeatureView        | FeatureView         | Delete                 | Delete a feature view from the registry         |
| GetStreamFeatureView     | StreamFeatureView   | Read                   | Get a stream feature view from the registry     |
| ListStreamFeatureViews   | StreamFeatureView   | Read                   | List stream feature views in the registry       |
| GetOnDemandFeatureView   | OnDemandFeatureView | Read                   | Get an on-demand feature view from the registry |
| ListOnDemandFeatureViews | OnDemandFeatureView | Read                   | List on-demand feature views in the registry    |
| ApplyFeatureService      | FeatureService      | Create, Update, Delete | Apply a feature service to the registry         |
| GetFeatureService        | FeatureService      | Read                   | Get a feature service from the registry         |
| ListFeatureServices      | FeatureService      | Read                   | List feature services in the registry           |
| DeleteFeatureService     | FeatureService      | Delete                 | Delete a feature service from the registry      |
| ApplySavedDataset        | SavedDataset        | Create, Update, Delete | Apply a saved dataset to the registry           |
| GetSavedDataset          | SavedDataset        | Read                   | Get a saved dataset from the registry           |
| ListSavedDatasets        | SavedDataset        | Read                   | List saved datasets in the registry             |
| DeleteSavedDataset       | SavedDataset        | Delete                 | Delete a saved dataset from the registry        |
| ApplyValidationReference | ValidationReference | Create, Update, Delete | Apply a validation reference to the registry    |
| GetValidationReference   | ValidationReference | Read                   | Get a validation reference from the registry    |
| ListValidationReferences | ValidationReference | Read                   | List validation references in the registry      |
| DeleteValidationReference| ValidationReference | Delete                 | Delete a validation reference from the registry |
| ApplyPermission          | Permission          | Create, Update, Delete | Apply a permission to the registry           |
| GetPermission            | Permission          | Read                   | Get a permission from the registry           |
| ListPermissions          | Permission          | Read                   | List permissions in the registry             |
| DeletePermission         | Permission          | Delete                 | Delete a permission from the registry        |
| Commit                   |                     | None                   | Commit changes to the registry               |
| Refresh                  |                     | None                   | Refresh the registry                         |
| Proto                    |                     | None                   | Get the proto representation of the registry |

## How to configure Authentication and Authorization
Please refer the [page](./../../../docs/getting-started/concepts/permission.md) for more details on how to configure authentication and authorization.

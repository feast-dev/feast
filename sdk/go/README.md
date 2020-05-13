# Feast Golang SDK

The Feast golang SDK currently only supports retrieval from online stores.

## Quickstart
```{go}
import (
    "context"
    feast "github.com/gojek/feast/sdk/go"
)

func main() {
    cli, err := feast.NewGrpcClient("localhost", 6565)
    if err != nil {
        panic(err)
    }
    
    ctx := context.Background()
    req := feast.OnlineFeaturesRequest{
        Features: []string{"my_project_1/feature1", "my_project_2/feature1", "my_project_4/feature3", "feature2", "feature2"},
        Entities: []feast.Row{
            {"entity1": feast.Int64Val(1), "entity2": feast.StrVal("bob")},
            {"entity1": feast.Int64Val(1), "entity2": feast.StrVal("annie")},
            {"entity1": feast.Int64Val(1), "entity2": feast.StrVal("jane")},
        },
        Project: "my_project_3",
    }

    resp, err := cli.GetOnlineFeatures(ctx, &req)
    if err != nil {
        panic(err)
    }

    // returns a list of rows (map[string]featureValue)
    out := resp.Rows()
}

```

If all features retrieved are of a single type, Feast provides convenience functions to retrieve your features as a vector of feature values:
```{go}
arr, err := resp.Int64Arrays(
    []string{"my_project_1/feature1", 
            "my_project_2/feature1", 
            "my_project_4/feature3", 
            "feature2", 
            "feature2"},             // order of features
    []int64{1,2,3,4,5})              // fillNa values
```

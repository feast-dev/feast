import click
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.logger import logger
from google.protobuf.json_format import MessageToDict, Parse

import feast
from feast import proto_json
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesRequest


def get_app(store: "feast.FeatureStore"):
    proto_json.patch()

    app = FastAPI()

    @app.post("/get-online-features")
    async def get_online_features(request: Request):
        try:
            # Validate and parse the request data into GetOnlineFeaturesRequest Protobuf object
            body = await request.body()
            request_proto = GetOnlineFeaturesRequest()
            Parse(body, request_proto)

            # Initialize parameters for FeatureStore.get_online_features(...) call
            if request_proto.HasField("feature_service"):
                features = store.get_feature_service(request_proto.feature_service)
            else:
                features = list(request_proto.features.val)

            full_feature_names = request_proto.full_feature_names

            batch_sizes = [len(v.val) for v in request_proto.entities.values()]
            num_entities = batch_sizes[0]
            if any(batch_size != num_entities for batch_size in batch_sizes):
                raise HTTPException(status_code=500, detail="Uneven number of columns")

            entity_rows = [
                {k: v.val[idx] for k, v in request_proto.entities.items()}
                for idx in range(num_entities)
            ]

            response_proto = store.get_online_features(
                features, entity_rows, full_feature_names=full_feature_names
            ).proto

            # Convert the Protobuf object to JSON and return it
            return MessageToDict(response_proto, preserving_proto_field_name=True)
        except Exception as e:
            # Print the original exception on the server side
            logger.exception(e)
            # Raise HTTPException to return the error message to the client
            raise HTTPException(status_code=500, detail=str(e))

    return app


def start_server(store: "feast.FeatureStore", port: int):
    app = get_app(store)
    click.echo(
        "This is an "
        + click.style("experimental", fg="yellow", bold=True, underline=True)
        + " feature. It's intended for early testing and feedback, and could change without warnings in future releases."
    )
    uvicorn.run(app, host="127.0.0.1", port=port)

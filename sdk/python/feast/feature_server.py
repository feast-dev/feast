import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.logger import logger
from google.protobuf.json_format import MessageToDict, Parse

import feast
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesRequest
from feast.type_map import feast_value_type_to_python_type


def get_app(store: "feast.FeatureStore"):
    app = FastAPI()

    @app.get("/get-online-features/")
    async def get_online_features(request: Request):
        try:
            # Validate and parse the request data into GetOnlineFeaturesRequest Protobuf object
            body = await request.body()
            request_proto = GetOnlineFeaturesRequest()
            Parse(body, request_proto)

            # Initialize parameters for FeatureStore.get_online_features(...) call
            if request_proto.HasField("feature_service_name"):
                features = store.get_feature_service(request_proto.feature_service_name)
                full_feature_names = False
            else:
                features = request_proto.feature_list_config.features
                full_feature_names = (
                    request_proto.feature_list_config.full_feature_names
                )

            entity_rows = [
                {
                    k: feast_value_type_to_python_type(v)
                    for k, v in entity_row_proto.fields.items()
                }
                for entity_row_proto in request_proto.entity_rows
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
    uvicorn.run(app, host="127.0.0.1", port=port)

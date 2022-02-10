import traceback

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.logger import logger
from fastapi.params import Depends
from google.protobuf.json_format import MessageToDict, Parse

import feast
from feast import proto_json
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesRequest


def get_app(store: "feast.FeatureStore"):
    proto_json.patch()

    app = FastAPI()

    async def get_body(request: Request):
        return await request.body()

    @app.post("/get-online-features")
    def get_online_features(body=Depends(get_body)):
        try:
            # Validate and parse the request data into GetOnlineFeaturesRequest Protobuf object
            request_proto = GetOnlineFeaturesRequest()
            Parse(body, request_proto)

            # Initialize parameters for FeatureStore.get_online_features(...) call
            if request_proto.HasField("feature_service"):
                features = store.get_feature_service(
                    request_proto.feature_service, allow_cache=True
                )
            else:
                features = list(request_proto.features.val)

            full_feature_names = request_proto.full_feature_names

            batch_sizes = [len(v.val) for v in request_proto.entities.values()]
            num_entities = batch_sizes[0]
            if any(batch_size != num_entities for batch_size in batch_sizes):
                raise HTTPException(status_code=500, detail="Uneven number of columns")

            response_proto = store._get_online_features(
                features,
                request_proto.entities,
                full_feature_names=full_feature_names,
                native_entity_values=False,
            ).proto

            # Convert the Protobuf object to JSON and return it
            return MessageToDict(  # type: ignore
                response_proto, preserving_proto_field_name=True, float_precision=18
            )
        except Exception as e:
            # Print the original exception on the server side
            logger.exception(traceback.format_exc())
            # Raise HTTPException to return the error message to the client
            raise HTTPException(status_code=500, detail=str(e))

    return app


def start_server(
    store: "feast.FeatureStore", host: str, port: int, no_access_log: bool
):
    app = get_app(store)
    uvicorn.run(app, host=host, port=port, access_log=(not no_access_log))

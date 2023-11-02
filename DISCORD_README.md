## Proto Hacks
We don't automatically run the `make` step since it's difficult to tie in with Bazel.
So we have to generate the protos and submit ourselves.

`make protos -i`

- make a venv
- pip install -e .
- nvm install sdk/python/requirements
- pip install -r

### FAQ
If you run into something along the lines of
```sh
make: [Makefile:50: compile-protos-python] Error 1 (ignored)
rm -rf  /home/discord/cdavid/feast/dist/grpc
mkdir -p dist/grpc;
cd /home/discord/cdavid/feast/protos && protoc --docs_out=../dist/grpc feast/*/*.proto
google/protobuf/duration.proto: File not found.
feast/core/Aggregation.proto:8:1: Import "google/protobuf/duration.proto" was not found or had errors.
feast/core/Aggregation.proto:13:5: "google.protobuf.Duration" is not defined.
feast/core/Aggregation.proto:14:5: "google.protobuf.Duration" is not defined.
make: [Makefile:438: compile-protos-docs] Error 1 (ignored)
```

(I'm not sure if this is the right way to go about this but it worked so :shrug:)
You'll need to go download the proto zip and move it into includes/
```sh
curl -OL https://github.com/google/protobuf/releases/download/v3.2.0/protoc-3.2.0-linux-x86_64.zip
unzip protoc-3.2.0-linux-x86_64.zip -d protoc3
mv protoc3/bin/* /usr/local/bin/
mv protoc3/include/* /usr/local/include/
```

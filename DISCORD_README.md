## Generating protos
Make sure you're in a venv \
Try to install the setup.py
```sh
pip install --upgrade pip
pip install -e .
```
If you run into weird proto backen mismatch issues run the following (make sure the versions here match still match the `setup_requires` section in `setup.py`)
```sh
pip install 'grpcio==1.48.1' 'grpcio-tools==1.48.1' 'mypy-protobuf==1.24' 'protobuf<3.20'
```

Install protoc-gen-doc \
For some reason the makefile is looking for `docs` so we also need to move it to `protoc-gen-docs`. \
Chose to do that because I suspect touching the makefile would create some fun merge conflicts down the line that future me doesn't want to deal with.
```sh
go get -u github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc
export PATH=$PATH:$HOME/go/bin
mv $HOME/go/bin/protoc-gen-doc $HOME/go/bin/protoc-gen-docs
```
Actually generate the protos
```sh
make protos -i
```
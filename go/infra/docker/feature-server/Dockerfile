FROM golang:1.23.12

# Update the package list and install the ca-certificates package
RUN apt-get update && apt-get install -y ca-certificates
RUN apt install -y protobuf-compiler

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

# Set the current working directory inside the container
WORKDIR /app

# Copy the source code into the container
COPY go/ ./go/
COPY go.mod go.sum ./

# Compile Protobuf files
COPY protos/ ./protos/
RUN mkdir -p go/protos
RUN find ./protos -name "*.proto" \
    -exec protoc --proto_path=protos --go_out=go/protos --go_opt=module=github.com/feast-dev/feast/go/protos --go-grpc_out=go/protos --go-grpc_opt=module=github.com/feast-dev/feast/go/protos {} \;

# Build the Go application
RUN go build -o feast ./go/main.go

# Expose ports
EXPOSE 8080

# Command to run the executable
# Pass arguments to the executable (Ex: ./feast --type=grpc)
CMD ["./feast"]
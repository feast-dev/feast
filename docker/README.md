# Docker

This directory is for building the dockerfiles for each of the feast components.

## Building the image

You can manually build the images by first compiling the jars:
```
mvn clean build
```

Then build the images:
```
docker build -t ${registry}/feast-core:${version} -f docker/core/Dockerfile .
docker build -t ${registry}/feast-serving:${version} -f docker/serving/Dockerfile .
```

All steps should be done from the root feast directory. Alternatively, you can run:
```
make build-docker registry=${registry} version=${version}
```
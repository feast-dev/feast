# Install and run a Feature Store on Kubernetes with the Feast Operator

The following notebooks will guide you through how to install and use Feast on Kubernetes with the Feast Go Operator.

* [01-Install.ipynb](./01-Install.ipynb): Install and configure a Feature Store in Kubernetes with the Operator.
* [02-Demo.ipynb](./02-Demo.ipynb): Validate the feature store with demo application.
* [03-Uninstall.ipynb](./03-Uninstall.ipynb): Clear the installed deployments.

# Here are the steps to deploy

* Generate certificates and create kubernetes secrets.
* helm install postgresql bitnami/postgresql -f values.yaml
* kubectl apply -f feast.yaml


# Some of the important commands to debug while setting up the postgres in TLS setup.
helm show chart bitnami/postgresql

helm install postgresql -f values.yaml bitnami/postgresql --version 16.4.6

helm uninstall postgresql

make test
make docker-build docker-push IMG=quay.io/lrangine/feast-operator:0.53.0
make install
make deploy IMG=quay.io/lrangine/feast-operator:0.53.0

kubectl exec -it postgresql-0 -- env PGPASSWORD=secret psql -U admin -d mydb -c "SHOW ssl;"

kubectl exec -it postgresql-0 -- env PGPASSWORD=password psql -U admin -d mydatabase -c "SHOW ssl;"


kubectl exec -it my-postgres-postgresql-0 -- bash



kubectl create secret generic postgres-tls-secret \
--from-file=tls.crt=postgres-certs/server.crt \
--from-file=tls.key=postgres-certs/server.key \
--from-file=ca.crt=postgres-certs/ca.crt

kubectl create secret generic postgres-tls-certs-new \
--from-file=tls.crt=postgres-tls-certs-new/server.crt \
--from-file=tls.key=postgres-tls-certs-new/server.key \
--from-file=ca.crt=postgres-tls-certs-new/ca.crt



kubectl run -it --rm --image=postgres:latest --restart=Never postgresql-client -- psql "host=postgresql.default.svc.cluster.local dbname=mydatabase user=admin password=mypassword sslmode=require"


kubectl exec -it postgresql-client -- psql \
"host=postgresql.default.svc.cluster.local dbname=mydatabase user=admin password=mypassword sslmode=verify-full sslcert=/etc/ssl/postgres/client.crt sslkey=/etc/ssl/postgres/client.key sslrootcert=/etc/ssl/postgres/ca.crt"

postgresql+psycopg://admin:password@postgresql.default.svc.cluster.local:5432/mydb?sslmode=require&sslrootcert=postgres-tls-certs/ca.crt&sslcert=postgres-tls-certs/client.crt&sslkey=postgres-tls-certs/client.key


kubectl exec -it postgresql-0 -- cat /opt/bitnami/postgresql/conf/postgresql.conf | grep ssl

sslcert=postgres-tls-certs-new/server.crt&sslkey=postgres-tls-certs-new/server.key

postgresql+psycopg://admin:password@localhost:5432/mydatabase?sslmode=disable&sslrootcert=postgres-tls-certs-new/ca.crt&sslcert=postgres-tls-certs-new/server.crt&sslkey=postgres-tls-certs-new/server.key


kubectl exec -it postgresql-0 -- env PGPASSWORD=password psql -U admin -d mydatabase -c '\l'


helm install postgresql -f values.yaml bitnami/postgresql --version 16.4.6

helm uninstall postgresql

helm template postgresql bitnami/postgresql --values values.yaml --version 16.4.6 > postgresql-export.yaml


kubectl create secret generic postgresql-client-certs \
--from-file=ca.crt=./postgres-tls-certs/ca.crt \
--from-file=tls.crt=./postgres-tls-certs/client.crt \
--from-file=tls.key=./postgres-tls-certs/client.key \
--dry-run=client -o yaml | kubectl apply -f -


!kubectl exec deploy/postgres -- psql -h localhost -U feast feast -c '\dt'

kubectl exec -it postgresql-0 -- env PGPASSWORD=password psql -U admin -d mydatabase -c '\dt'

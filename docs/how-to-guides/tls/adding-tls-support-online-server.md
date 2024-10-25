# Adding TLS(SSL) to feature store server AKA online server.

### Obtain an SSL certificate and key

In development mode we can generate self-signed certificate for testing purpose. In actual production environment it is always recommended to get it from the popular SSL certificate providers.


```shell
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes
```

The above command will generate two files
* `key.pem` : certificate private key
* `cert.pem`: certificate public key
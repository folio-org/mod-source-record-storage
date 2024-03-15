# TLS Test
This is automated in the .github/workflows/tlstest.yml GitHub Action.

Use one of these openssl commands to create new server.key and server.crt.

```
openssl req -newkey rsa:2048 -keyout server.key -x509 -days 365 -out server.crt -subj "/CN=postgres" -nodes
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:secp256r1 -days 3650 -nodes -keyout server.key -out server.crt -subj "/CN=postgres"
```

Provide the certificate as docker-compose environment variable.
```
(echo 'SERVER_CRT="'; cat server.crt; echo '"') > .env
```

Run the containers in the first terminal.
```
docker-compose rm -f ; docker-compose build --no-cache ; docker-compose up
```

Enable the module for tenant diku in the second terminal.
```
wget -S -O - --header "Content-type: application/json" --header "x-okapi-tenant: diku" --post-data '{"module_to": "mod-source-record-storage-99.0.0"}' http://localhost:8081/_/tenant
```

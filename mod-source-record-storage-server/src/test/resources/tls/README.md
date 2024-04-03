# TLS Test
Use one of these openssl commands to create new server.key and server.crt.

The IP 172.17.0.1 is needed for Jenkins CI: https://java.testcontainers.org/features/networking/#getting-the-container-host

```
openssl req -x509 -days 3650 -subj "/CN=postgres" -addext 'subjectAltName=DNS:localhost,IP:172.17.0.1' -nodes -keyout server.key -out server.crt -newkey rsa:2048
```

```
openssl req -x509 -days 3650 -subj "/CN=postgres" -addext 'subjectAltName=DNS:localhost,IP:172.17.0.1' -nodes -keyout server.key -out server.crt -newkey ec -pkeyopt ec_paramgen_curve:prime256v1
```

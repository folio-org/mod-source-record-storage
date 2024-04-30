# TLS Test
Use one of these openssl commands to create new server.key and server.crt.

```
openssl req -x509 -days 3650 -subj "/CN=postgres" -nodes -keyout server.key -out server.crt -newkey rsa:2048
openssl req -x509 -days 3650 -subj "/CN=postgres" -nodes -keyout server.key -out server.crt -newkey ec -pkeyopt ec_paramgen_curve:prime256v1
```

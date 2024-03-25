FROM docker.io/library/postgres:12-alpine
COPY --chown=postgres --chmod=0400 server.key server.crt /var/lib/postgresql/
COPY hostssl.sh /docker-entrypoint-initdb.d/

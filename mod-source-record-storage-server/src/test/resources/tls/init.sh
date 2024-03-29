#!/bin/sh

# cp creates the files with 'postgres' ownership
cp /server.* /var/lib/postgresql/data/
chmod 400 /var/lib/postgresql/data/server.*

# disallow connections without SSL/TLS
echo 'hostssl all all all md5' > /var/lib/postgresql/data/pg_hba.conf

# enable SSL
echo 'ssl = on' >> /var/lib/postgresql/data/postgresql.conf

exit 0

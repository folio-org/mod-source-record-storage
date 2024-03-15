# disable connections without SSL/TLS
echo 'hostssl all all all md5' > /var/lib/postgresql/data/pg_hba.conf

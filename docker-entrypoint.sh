#!/bin/bash 
echo "Hello World!"

function do_exit() {
  echo "Exiting ..."
  /etc/init.d/postgresql stop
  echo "Done."
}
trap do_exit SIGTERM SIGINT SIGHUP

/etc/init.d/postgresql start

sudo -u postgres psql <<EOF
create user admin password 'letmein';
create database mydb with owner = admin;
EOF

flyway migrate

echo "Postgresql started."

sleep infinity &
wait $!

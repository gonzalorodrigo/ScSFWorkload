#!/bin/bash
#
# Creates a database called scsf and user scsf that can access it locally
# with password scsf-pass. It uses root as user and will ask a password from
# the user.
#


mysql_user="root"
db_name="scsf"
echo "This script creates a MySQL database (${db_name}) as user ${mysql_user}".
echo "It will request the password of such user twice."
scsf_user="scsf"
scsf_pass="scsf-pass"
echo "create database ${db_name};"\
"GRANT ALL PRIVILEGES ON ${db_name}.* TO ${scsf_user}@localhost" \
" IDENTIFIED BY '${scsf_pass}'" | mysql -u "${mysql_user}" -p

test_db_name="scsftest"
test_user="testscsf"
test_pass="testscsf-pass"
echo "create database ${test_db_name};"\
"GRANT ALL PRIVILEGES ON ${test_db_name}.* TO ${test_user}@localhost" \
" IDENTIFIED BY '${test_pass}'"
echo "create database ${test_db_name};"\
"GRANT ALL PRIVILEGES ON ${test_db_name}.* TO ${test_user}@localhost" \
" IDENTIFIED BY '${test_pass}'" | mysql -u "${mysql_user}" -p
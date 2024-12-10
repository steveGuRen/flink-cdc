docker run --name mysql \
  -e MYSQL_ROOT_PASSWORD=abcd1234 \
  -p 3306:3306 \
  -v /mnt/data1/mysql/my.cnf:/etc/mysql/my.cnf \
  -d mysql:8.0

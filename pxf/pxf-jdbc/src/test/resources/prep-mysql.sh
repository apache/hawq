sudo yum install mysql-server

sudo service mysqld start

mysql -u root

# Set password for root user and drop anonymous logins

SET PASSWORD FOR 'root'@'localhost' = PASSWORD('secret');
SET PASSWORD FOR 'root'@'127.0.0.1' = PASSWORD('secret');
FLUSH PRIVILEGES;

DROP USER ''@'localhost';
DROP USER ''@'%';
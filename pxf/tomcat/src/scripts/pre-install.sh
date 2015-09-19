#!/bin/sh

# Pre installation script

user=tomcat
group=tomcat

groupadd=/usr/sbin/groupadd
useradd=/usr/sbin/useradd
usermod=/usr/sbin/usermod

# Create system group tomcat if doesn't exist
getent group $group > /dev/null || $groupadd -r $group

# Create system user tomcat if doens't exist
getent passwd $user > /dev/null || $useradd --comment "tomcat server user" -M -r -g $group $user

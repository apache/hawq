#!/bin/sh

# Pre installation script

user=pxf
group=pxf
tcgroup=tomcat

groupadd=/usr/sbin/groupadd
useradd=/usr/sbin/useradd
usermod=/usr/sbin/usermod

if [ -f /etc/redhat-release ]; then
  system=redhat
elif [ -f /etc/SuSE-release ]; then
  system=suse
fi

# Create system group hadoop if doesn't exist
getent group hadoop > /dev/null || $groupadd -r hadoop

# Create system group pxf if doesn't exist
getent group $group > /dev/null || $groupadd -r $group

# Create system user pxf if doens't exist
getent passwd $user > /dev/null || $useradd --comment "PXF service user" -M -r -g $group -G hadoop $user

# Add pxf user to tomcat group so it can control the instance
if [ "$system" == "suse" ]; then
  getent group $tcgroup > /dev/null && $usermod -A $tcgroup $user
else
  getent group $tcgroup > /dev/null && $usermod -a -G $tcgroup $user
fi
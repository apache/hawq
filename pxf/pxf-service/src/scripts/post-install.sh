#!/bin/sh

# post installation script

user=pxf
group=pxf

groupadd=/usr/sbin/groupadd
useradd=/usr/sbin/useradd
usermod=/usr/sbin/usermod

# Create system group hadoop if doesn't exist
getent group hadoop > /dev/null || $groupadd -r hadoop

# Create system group pxf if doesn't exist
getent group $group > /dev/null || $groupadd -r $group

# Create system user pxf if doens't exist
getent passwd $user > /dev/null || $useradd --comment "PXF service user" -M -r -g $group -G hadoop $user

# Add pxf user to vfabric group so it can control the instance
getent group vfabric > /dev/null && $usermod -a -G vfabric $user
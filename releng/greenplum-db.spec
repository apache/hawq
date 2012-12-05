#
# RPM spec file for GPSQL
# Uses the greenplum-db-*.bin file that contains both the shell script to install GPDB as well as the tarball contents
# Currently, expects the greenplum-db-*.bin file to be present in the same directory as the spec file.
#
# TODOS:
# 1. Can the RPM also own and manage the greenplum database data directory?
# 2. Can the RPM also own and manage the cluster configuration file?
# 3. Are there any disadvantages of turning AutoReq off, as has been done below?
#

%define name            gpsql
%define gpdbname        greenplum-db
%define version         %{greenplum_db_ver}
%define arch            x86_64
%if "%{version}" == "dev"
%define gpdbtarball  %{gpdbname}-%{version}-RHEL5-%{arch}.tar.gz
%define release         dev
%else
%define gpdbtarball  %{gpdbname}-%{version}-build-%{release}-RHEL5-%{arch}.tar.gz
%define release         %{bld_number}
%endif
%define installdir      /usr/local/%{name}-%{version}
%define symlink         /usr/local/%{name}

Summary:        GPSQL
Name:           %{name}
Version:        %{version}
Release:        %{release}
License:        2012, Greenplum an EMC division
Group:          Applications/Databases
URL:            http://www.greenplum.com/products/greenplum-database
BuildArch:      %{arch}
# This prevents rpmbuild from generating automatic dependecies on libraries and
# binaries. Some of these dependencies cause problems while installing the
# generated RPMS. However, we tested the rpm generated after turning AutoReq off
# and it seemed to work fine.
AutoReq:        no
BuildRoot:      %{_topdir}/temp
Prefix:         /usr/local

%description
GPSQL

%prep
# As the source tarball for this RPM is created during the %%prep phase, we cannot assign the source as SOURCE0: <tar.gz file path>
# To get around this issue, using the command below to manually copy the tar.gz file into the SOURCES directory
cp ../../%{gpdbtarball} %_sourcedir/.

%build

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT%{installdir}
tar zxf %{_sourcedir}/%{gpdbtarball} -C $RPM_BUILD_ROOT%{installdir}
ln -s %{installdir} $RPM_BUILD_ROOT%{symlink}

%clean
rm -rf $RPM_BUILD_ROOT

%files
%{installdir}
%{symlink}

%post
# Update GPHOME in greenplum_path.sh
# Have to use sed to replace it into another file, and then move it back to greenplum_path.sh
# Made sure that even after this step, rpm -e removes greenplum_path.sh as well
sed "s#GPHOME=.*#GPHOME=%{installdir}#g" %{installdir}/greenplum_path.sh > %{installdir}/greenplum_path.sh.updated
mv %{installdir}/greenplum_path.sh.updated %{installdir}/greenplum_path.sh


## ======================================================================
## RPM spec file for HAWQ
##
##  o Currently, expects the greenplum-db-*.tar.gz file to be present
##    in source working directory root.
## ======================================================================

# disable stripping of debug symbols
%global _enable_debug_package 0
%global debug_package %{nil}
%global __os_install_post /usr/lib/rpm/brp-compress %{nil}

%define name            hawq
%define gpdbname        greenplum-db
%define version         %{greenplum_db_ver}
%define arch            x86_64

%if "%{version}" == "dev"
%define gptarball  %{gpdbname}-%{version}-RHEL5-%{arch}.tar.gz
%define release         dev
%else
%define gptarball  %{gpdbname}-%{version}-build-%{release}-RHEL5-%{arch}.tar.gz
%define release         %{bld_number}
%endif

%define installdir      /usr/local/%{name}-%{version}
%define symlink         /usr/local/%{name}

Summary:        HAWQ, the power behind Pivotal Advanced Database Services (ADS)
Name:           %{name}
Version:        %{version}
Release:        %{release}
License:        Copyright (c) 2013, EMC Greenplum
Vendor:         EMC Greenplum
Group:          Applications/Databases
URL:            http://www.greenplum.com/products/pivotal-hd
BuildArch:      %{arch}

# This prevents rpmbuild from generating automatic dependecies on libraries and
# binaries. Some of these dependencies cause problems while installing the
# generated RPMS. However, we tested the rpm generated after turning AutoReq off
# and it seemed to work fine.
AutoReq:        no

BuildRoot:      %{_topdir}/temp
Prefix:         /usr/local

%description
Pivotal Advanced Database Services (ADS) powered by HAWQ, extends
Pivotal HD Enterprise, adding rich, proven parallel SQL processing
facilities. These render Hadoop queries faster than any Hadoop-based
query interface on the market today, enhancing productivity. Pivotal
ADS enables SQL analysis of data in a variety of Hadoop-based data
formats using the Pivotal Xtension Framework, without duplicating or
converting HBase files. Alternatively, an optimized format is
available for ADS table storage for best performance.

%prep
# As the source tarball for this RPM is created during the %%prep phase, we cannot assign the source as SOURCE0: <tar.gz file path>
# To get around this issue, using the command below to manually copy the tar.gz file into the SOURCES directory
cp ../../%{gptarball} %_sourcedir/.

%build

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT%{installdir}
tar zxf %{_sourcedir}/%{gptarball} -C $RPM_BUILD_ROOT%{installdir}
(cd $RPM_BUILD_ROOT%{installdir}/..; ln -s %{name}-%{version} %{name})

#disable stripping of debug symbols
export DONT_STRIP=1

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-, gpadmin, gpadmin, -)
%{installdir}
%{symlink}

%post
INSTDIR=$RPM_INSTALL_PREFIX0/%{name}-%{version}
# Update GPHOME in greenplum_path.sh
sed "s|^GPHOME=.*|GPHOME=${INSTDIR}|g" -i ${INSTDIR}/greenplum_path.sh

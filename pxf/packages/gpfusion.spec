#
# RPM Spec file for GPFusion version @version@
#

%define _name		gpfusion
%define _version	@version@
%define _package	@package.release@
%define _prefix		@package.prefix@

# Build time settings
%define _final_name @final.name@

Name: %{_name}
Version: %{_version}
Release: %{_package}
Summary: The GPFusion GPDB-Hadoop fusion library
License: Apache License v2.0
Source: %{_final_name}.tar.gz
Vendor: Greenplum
Buildarch: noarch
Requires: hbase, zookeeper, hadoop
AutoReqProv: no
Provides: gpfusion
Prefix: %{_prefix}

%description
GPFusion provides all kinds of stuff and very cool amazing magical features

%prep
%setup -n %{_final_name}

%install
if [ -d $RPM_BUILD_ROOT/%{_prefix} ]; then
	rm -rf $RPM_BUILD_ROOT/%{_prefix}
fi

mkdir -p $RPM_BUILD_ROOT/%{_prefix}/%{_final_name}
cp *.jar $RPM_BUILD_ROOT/%{_prefix}/%{_final_name}

%files
%{_prefix}

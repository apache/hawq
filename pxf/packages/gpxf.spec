#
# RPM Spec file for GPXF version @version@
#

%define _name			gpxf
%define _version		@version@
%define _package		@package.release@
%define _prefix			@package.prefix@
%define _publicstage	publicstage

# Build time settings
%define _final_name @final.name@

Name: %{_name}
Version: %{_version}
Release: %{_package}
Summary: The GPXF GPDB-Hadoop fusion library
License: Apache License v2.0
Source: %{_final_name}-%{_package}.tar.gz
Vendor: Greenplum
Buildarch: noarch
Requires: hadoop >= 2.0.0
AutoReqProv: no
Provides: gpxf
Prefix: %{_prefix}

%description
GPXF provides all kinds of stuff and very cool amazing magical features

%prep
%setup -n %{_final_name}

%install
if [ -d $RPM_BUILD_ROOT/%{_prefix}/%{_final_name} ]; then
	rm -rf $RPM_BUILD_ROOT/%{_prefix}/%{_final_name}
fi

mkdir -p $RPM_BUILD_ROOT/%{_prefix}/%{_final_name}
cp *.jar $RPM_BUILD_ROOT/%{_prefix}/%{_final_name}

%post

if [ ! -d $RPM_BUILD_ROOT/%{_prefix}/%{_publicstage} ]; then
	mkdir -p $RPM_BUILD_ROOT/%{_prefix}/%{_publicstage}
	getent passwd hdfs > /dev/null && chown hdfs $RPM_BUILD_ROOT/%{_prefix}/%{_publicstage}
	chmod 777 $RPM_BUILD_ROOT/%{_prefix}/%{_publicstage}
fi

pushd $RPM_BUILD_ROOT/%{prefix} > /dev/null
if [ ! -e %{_name} ]; then
	ln -s %{_final_name} %{_name}
fi

cd %{_final_name}
ln -s %{_final_name}.jar %{name}.jar
popd > /dev/null

%preun

pushd $RPM_BUILD_ROOT/%{prefix} > /dev/null
if [ -h %{_name} ]; then
	rm %{_name}
fi

rmdir --ignore-fail-on-non-empty %{_publicstage}

cd %{_final_name}
if [ -h %{_name}.jar ]; then
	rm %{_name}.jar
fi

popd > /dev/null

%files
%{_prefix}

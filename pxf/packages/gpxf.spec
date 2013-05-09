#
# RPM Spec file for GPXF version @version@
#

# Build time settings, replaced by build.xml
%define _name				@component.basename@
%define _publicstage		@package.publicstage@
%define _version			@component.version@
%define _package_release	@package.release@
%define _prefix				@package.prefix@
%define _component_name		@component.name@
%define _package_name		@package.name@
%define _package_summary	@package.summary@
%define _package_vendor		@package.vendor@

Name: %{_name}
Version: %{_version}
Release: %{_package_release}
Summary: %{_package_summary}
License: Apache License v2.0
Source: %{_package_name}.tar.gz
Vendor: %{_package_vendor}
Buildarch: noarch
Requires: hadoop >= 2.0.0
AutoReqProv: no
Provides: %{_name}
Prefix: %{_prefix}

# package.description is replaced here and not in header as
# description area cannot use %{}
%description
@package.description@

%prep
%setup -n %{_component_name}

%install
if [ -d $RPM_BUILD_ROOT/%{_prefix}/%{_package_name} ]; then
	rm -rf $RPM_BUILD_ROOT/%{_prefix}/%{_package_name}
fi

mkdir -p $RPM_BUILD_ROOT/%{_prefix}/%{_package_name}
cp *.jar $RPM_BUILD_ROOT/%{_prefix}/%{_package_name}

%post

if [ ! -d $RPM_BUILD_ROOT/%{_prefix}/%{_publicstage} ]; then
	mkdir -p $RPM_BUILD_ROOT/%{_prefix}/%{_publicstage}
	getent passwd hdfs > /dev/null && chown hdfs $RPM_BUILD_ROOT/%{_prefix}/%{_publicstage}
	chmod 777 $RPM_BUILD_ROOT/%{_prefix}/%{_publicstage}
fi

pushd $RPM_BUILD_ROOT/%{prefix} > /dev/null
if [ -h %{_name} ]; then
	rm %{_name}
fi
ln -s %{_package_name} %{_name}

cd %{_package_name}
ln -s %{_component_name}.jar %{_name}.jar
popd > /dev/null

%preun

pushd $RPM_BUILD_ROOT/%{prefix} > /dev/null

if [ -h %{_name} -a "`readlink %{_name} | xargs basename`" == "%{_package_name}" ]; then
	rm %{_name}
fi

cd %{_package_name}
if [ -h %{_name}.jar ]; then
	rm %{_name}.jar
fi

popd > /dev/null

%files
%{_prefix}

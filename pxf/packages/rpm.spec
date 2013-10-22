#
# RPM Spec file for PXF version @version@
#

# Build time settings, replaced by build.xml
%define _name				@component.basename@
%define _publicstage		@package.publicstage@
%define _version			@component.version@
%define _package_release	@package.release@
%define _lib_prefix			@package.libprefix@
%define _etc_prefix			@package.etcprefix@
%define _component_name		@component.name@
%define _package_name		@package.name@
%define _package_summary	@package.summary@
%define _package_vendor		@package.vendor@
%define _package_obsoletes	@package.obsoletes@

Name: %{_name}
Version: %{_version}
Release: %{_package_release}
Summary: %{_package_summary}
License: Copyright (c) 2013, EMC Greenplum
Group: Applications/Databases
Source: %{_package_name}.tar.gz
Vendor: %{_package_vendor}
Buildarch: noarch
Requires: hadoop >= 2.0.0, hadoop-mapreduce >= 2.0.0
AutoReqProv: no
Provides: %{_name}
BuildRoot: %{_topdir}/temp
Obsoletes: %{_package_obsoletes}

# package.description is replaced here and not in header as
# description area cannot use %{}
%description
@package.description@

%prep
%setup -n %{_component_name}

%install
if [ -d $RPM_BUILD_ROOT/%{_lib_prefix}/%{_package_name} ]; then
	rm -rf $RPM_BUILD_ROOT/%{_lib_prefix}/%{_package_name}
fi

mkdir -p $RPM_BUILD_ROOT/%{_lib_prefix}/%{_package_name}
cp *.jar $RPM_BUILD_ROOT/%{_lib_prefix}/%{_package_name}

if [ -d $RPM_BUILD_ROOT/%{_etc_prefix}/%{_package_name} ]; then
	rm -rf $RPM_BUILD_ROOT/%{_etc_prefix}/%{_package_name}
fi

mkdir -p $RPM_BUILD_ROOT/%{_etc_prefix}/%{_package_name}/conf
cp conf/* $RPM_BUILD_ROOT/%{_etc_prefix}/%{_package_name}/conf/

%post

if [ ! -d $RPM_BUILD_ROOT/%{_lib_prefix}/%{_publicstage} ]; then
	mkdir -p $RPM_BUILD_ROOT/%{_lib_prefix}/%{_publicstage}
	getent passwd hdfs > /dev/null && chown hdfs $RPM_BUILD_ROOT/%{_lib_prefix}/%{_publicstage}
	chmod 777 $RPM_BUILD_ROOT/%{_lib_prefix}/%{_publicstage}
fi

pushd $RPM_BUILD_ROOT/%{_lib_prefix} > /dev/null
if [ -h %{_name} ]; then
	rm %{_name}
fi
ln -s %{_package_name} %{_name}

cd %{_package_name}
ln -s %{_component_name}.jar %{_name}.jar
popd > /dev/null

pushd $RPM_BUILD_ROOT/%{_etc_prefix} > /dev/null
if [ -h %{_name} ] && [ "`readlink %{_name} | xargs basename`" != "%{_package_name}" ]; then
	if [ -f %{_name}/conf/pxf-profiles.xml ]; then
		echo pxf-profiles.xml replaced, old copy is `readlink -f %{_name}/conf/pxf_profiles.xml`
	fi
	rm %{_name}
fi

ln -s %{_package_name} %{_name}

popd > /dev/null

%preun

pushd $RPM_BUILD_ROOT/%{_lib_prefix} > /dev/null

if [ -h %{_name} -a "`readlink %{_name} | xargs basename`" == "%{_package_name}" ]; then
	rm %{_name}
fi

cd %{_package_name}
if [ -h %{_name}.jar ]; then
	rm %{_name}.jar
fi

popd > /dev/null

pushd $RPM_BUILD_ROOT/%{_etc_prefix} > /dev/null

if [ -h %{_name} -a "`readlink %{_name} | xargs basename`" == "%{_package_name}" ]; then
	rm %{_name}
fi

popd > /dev/null

%files
%{_lib_prefix}
%{_etc_prefix}

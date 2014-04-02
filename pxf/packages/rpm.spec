#
# RPM Spec file for PXF version @version@
#

# Build time settings, replaced by build.xml
%define _version			@comp.version@
%define _component_name		@comp.name@
%define _package_link		@comp.basename@
%define _dependencies	    @comp.deps@
%define _name				@pkg.filename@
%define _publicstage		@pkg.publicstage@
%define _package_release	@pkg.release@
%define _lib_prefix			@pkg.libprefix@
%define _etc_prefix			@pkg.etcprefix@
%define _package_name		@pkg.name@
%define _package_dir		@pkg.dir@
%define _package_summary	@pkg.summary@
%define _package_vendor		@pkg.vendor@
%define _package_obsoletes	@pkg.obsoletes@



Name: %{_name}
Version: %{_version}
Release: %{_package_release}
Summary: %{_package_summary}
License: Copyright (c) 2014, Pivotal
Group: Applications/Databases
Source: %{_package_name}.tar.gz
Vendor: %{_package_vendor}
Buildarch: noarch
Requires: %{_dependencies}
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
if [ -d $RPM_BUILD_ROOT/%{_lib_prefix}/%{_package_dir} ]; then
	rm -rf $RPM_BUILD_ROOT/%{_lib_prefix}/%{_package_dir}
fi

mkdir -p $RPM_BUILD_ROOT/%{_lib_prefix}/%{_package_dir}
cp *.jar $RPM_BUILD_ROOT/%{_lib_prefix}/%{_package_dir}

if [ -d $RPM_BUILD_ROOT/%{_etc_prefix}/%{_package_dir} ]; then
	rm -rf $RPM_BUILD_ROOT/%{_etc_prefix}/%{_package_dir}
fi

mkdir -p $RPM_BUILD_ROOT/%{_etc_prefix}/%{_package_dir}/conf
test -e conf/* && cp conf/* $RPM_BUILD_ROOT/%{_etc_prefix}/%{_package_dir}/conf/

%post

if [ ! -d $RPM_BUILD_ROOT/%{_lib_prefix}/%{_publicstage} ]; then
	mkdir -p $RPM_BUILD_ROOT/%{_lib_prefix}/%{_publicstage}
	getent passwd hdfs > /dev/null && chown hdfs $RPM_BUILD_ROOT/%{_lib_prefix}/%{_publicstage}
	chmod 777 $RPM_BUILD_ROOT/%{_lib_prefix}/%{_publicstage}
fi

pushd $RPM_BUILD_ROOT/%{_lib_prefix} > /dev/null
if [ -L %{_package_link} ]; then
	rm %{_package_link}
fi
ln -s %{_package_dir} %{_package_link}

cd %{_package_dir}
ln -s %{_component_name}.jar %{_name}.jar
popd > /dev/null

pushd $RPM_BUILD_ROOT/%{_etc_prefix} > /dev/null
if [ -L %{_package_link} ]; then
    if [ "`readlink %{_package_link} | xargs basename`" != "%{_package_dir}" ] && [ -f %{_package_link}/conf/pxf-profiles.xml ]; then
		echo pxf-profiles.xml replaced, old copy is `readlink -f %{_package_link}/conf/pxf_profiles.xml`
	fi
	rm %{_package_link}
fi

ln -s %{_package_dir} %{_package_link}

popd > /dev/null

%preun

pushd $RPM_BUILD_ROOT/%{_lib_prefix} > /dev/null


cd %{_package_dir}
if [ -L %{_name}.jar ]; then
	rm %{_name}.jar
fi

popd > /dev/null

%postun

pushd $RPM_BUILD_ROOT/%{_lib_prefix} > /dev/null

if [ ! -e %{_package_link} ]; then
	rm %{_package_link}
fi

popd > /dev/null

pushd $RPM_BUILD_ROOT/%{_etc_prefix} > /dev/null

if [ ! -e %{_package_link} ]; then
	rm %{_package_link}
fi

popd > /dev/null

%files
%{_lib_prefix}
%{_etc_prefix}

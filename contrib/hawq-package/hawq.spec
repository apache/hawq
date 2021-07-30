# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

%global    _enable_debug_package  0
%global    debug_package          %{nil}
%global    __os_install_post      /usr/lib/rpm/brp-compress %{nil}
%define    hawq_version           %{_hawq_version}
%define    rpm_os_version         %{_rpm_os_version}
%define    arch                   x86_64
%define    installdir             /usr/local/%{name}

Name:       apache-hawq
Summary:    Hadoop Native SQL powered by Apache HAWQ (incubating)
Version:    %{hawq_version}
Release:    %{rpm_os_version}
License:    ASL 2.0
Group:      Applications/Databases
URL:        http://hawq.incubator.apache.org
Prefix:     /usr/local
BuildArch:  %{arch}
SOURCE0 :   apache-hawq-src-%{hawq_version}-incubating.tar.gz
Requires:   libgsasl, krb5-libs, libicu, protobuf >= 2.5.0, json-c >= 0.9, net-snmp-libs, thrift >= 0.9.1, boost >= 1.53.0
%if %{rpm_os_version} == el6
Requires: openssl
%else
Requires: openssl-libs
%endif
Requires(pre): shadow-utils

AutoReqProv:    no

%description
Apache HAWQ (incubating) combines exceptional MPP-based analytics
performance, robust ANSI SQL compliance, Hadoop ecosystem
integration and manageability, and flexible data-store format
support, all natively in Hadoop, no connectors required.

Built from a decade’s worth of massively parallel
processing (MPP) expertise developed through the creation of open
source Greenplum® Database and PostgreSQL, HAWQ enables you to
swiftly and interactively query Hadoop data, natively via HDFS.

%prep
%setup -n %{name}-src-%{version}-incubating

%build
export CFLAGS="-O3 -g"
export CXXFLAGS="-O3 -g"
./configure --prefix=%{installdir} --with-pgport=5432 --with-libedit-preferred --enable-snmp \
            --with-perl --with-python --with-java --with-openssl --with-pam --without-krb5 \
            --with-gssapi --with-ldap --with-pgcrypto --enable-orca
core_count=$(grep -c 'core id' /proc/cpuinfo)
make -j"${core_count}"

%install
export DONT_STRIP=1
rm -rf %{buildroot}
make install DESTDIR="%{buildroot}"
# The buildroot directory should not exist in the binary file
sed -i "s|%{buildroot}||g" %{buildroot}%{installdir}/lib/python/pygresql/_pg.so

%pre
# Add the default "gpadmin" user and group if it does not exist
getent group  gpadmin >/dev/null || groupadd -r gpadmin
getent passwd gpadmin >/dev/null || useradd -m -r -g gpadmin -c "Apache HAWQ account" gpadmin
exit 0

%post
INSTDIR=$RPM_INSTALL_PREFIX0/%{name}
# Update GPHOME in greenplum_path.sh
sed "s|^GPHOME=.*|GPHOME=${INSTDIR}|g" -i ${INSTDIR}/greenplum_path.sh

%postun

%clean
rm -rf %{buildroot}

%files
%defattr(-, gpadmin, gpadmin, 0755)
%{installdir}
%config(noreplace) %{installdir}/etc/hawq-site.xml
%config(noreplace) %{installdir}/etc/hdfs-client.xml
%config(noreplace) %{installdir}/etc/yarn-client.xml

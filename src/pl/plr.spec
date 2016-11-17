Summary:        PL/R module for HAWQ
Name:           plr-hawq_%{hawq_version_str}
Version:        08.03.00.14.%{hawq_version}
Release:		%{hawq_build_number}.el%{redhat_major_version}
Prefix:         /usr/local/hawq_%{hawq_version_str}
License:		GPL
BuildRequires:  R-core-devel
Requires:       R
Provides:       plr-hawq

%define _unpackaged_files_terminate_build 0

AutoReqProv:    no

%description
The PL/R modules provides Procedural language implementation of R for HAWQ.

%install
# Note:
# 1. We do not use DESTDIR to specify the installation path since the binaries will be
#    installed in $DESTDIR/$prefix instead of $DESTDIR.
# 2. We build using the "configured" prefix at first and then install it using
#    the new prefix.
make -C %{plr_srcdir}
make -C %{plr_srcdir} install prefix=%{buildroot}/usr/local/hawq_%{hawq_version_str}

%files
/usr/local/hawq_%{hawq_version_str}/docs/contrib/README.plr
/usr/local/hawq_%{hawq_version_str}/lib/postgresql/plr.so*
/usr/local/hawq_%{hawq_version_str}/share/postgresql/contrib/plr.sql

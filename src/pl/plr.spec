Summary:        PL/R module for HAWQ
Name:           plr-hawq
Version:        08.03.00.14
Release:		0
Prefix:         /usr/local
License:		GPL

%description
The PL/R modules provides Procedural language implementation of R for HAWQ.

%install
make -C %{plr_srcdir} install prefix=%{buildroot}/usr/local

%files
/usr/local

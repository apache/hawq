%define maindir     %(echo `cd ../../../../..;pwd`)
%define _topdir     %{maindir}/cdb-pg/src/pl/plr/package 
%define name        R
%define release     0
%define version     2.13 
%define buildroot   %{_topdir}/R-%{version}.%{release} 
%define buildarch   %(uname -p) 
%define __os_install_post %{nil}

BuildRoot:      %{buildroot}
SUmmary:        R statistics language 
License:        GPLv2        
Name:           %{name}
Version:        %{version}
Release:        %{release} 
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
BuildArch:      %{buildarch} 
Provides:       R-%{version}-%{release}.%{buildarch}.rpm 

%description
The R module provides the R statistics language.

%prep
rm -rf %{buildroot}

%install
mkdir -p %{buildroot}/temp/share/packages/R
cp -r %{maindir}/ext/%{arch}/R-%{version}.%{release}/*   %{buildroot}/temp/share/packages/R

%files
/temp

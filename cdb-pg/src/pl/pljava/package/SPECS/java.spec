%define maindir     %(echo `cd ..//../../../..;pwd`)
%define _topdir     %{maindir}/cdb-pg/src/pl/pljava/package
%define name        java 
%define release     26 
%define version     1.6.0 
%define buildroot   %{_topdir}/java%{version}_%{release} 
%define buildarch   %(uname -p) 
%define __os_install_post %{nil}

BuildRoot:      %{buildroot}
Summary:        Java  
License:        Sun License       
Name:           %{name}
Version:        %{version}
Release:        %{release} 
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
BuildArch:      %{buildarch} 
Provides:       java-%{version}-%{release}.%{buildarch}.rpm 

%description
The Java package provides Java for PL/Java.

%prep
rm -rf %{buildroot}

%install
mkdir -p %{buildroot}/temp/share/packages/java
cp -r %{maindir}/ext/%{arch}/jre%{version}_%{release}/*   %{buildroot}/temp/share/packages/java

%files
/temp

Summary:        Geospatial extensions for Greenplum Database 
License:        GPLv2        
Name:           postgis
Version:        %{postgis_ver}
Release:        %{postgis_rel}
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
Provides:       postgis = %{postgis_ver}
Requires:       geos = %{geos_ver}, proj = %{proj_ver}

%description
The PostGIS module provides geospatial extensions for Greenplum Database.

%install
make -C %{postgis_dir} install prefix=%{buildroot}/temp 

%files
/temp

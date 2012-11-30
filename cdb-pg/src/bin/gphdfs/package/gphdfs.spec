Summary:        gphdfs for Greenplum Database
License:        EMC copyright        
Name:           gphdfs
Version:        %{gphdfs_ver}
Release:        %{gphdfs_rel}
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
BuildArch:      %{buildarch} 
Provides:       gphdfs = %{gphdfs_ver}
Requires:       jre = %{jre_ver}

%description
The gphdfs rpm provides gphdfs library for the Greenplum Database.

%install
mkdir -p %{buildroot}/temp
make -C %{gphdfs_dir} install prefix=%{buildroot}/temp

%files
/temp


Summary:        An abstract layer for external UDFs
License:        Commercial
Name:           libgppc
Version:        %{libgppc_ver}
Release:        %{libgppc_rel}
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
Provides:       libgppc = %{libgppc_ver}

%description
Libgppc provides an abstract layer for external UDFs

%install
mkdir -p %{buildroot}/temp
make -C %{libgppc_dir} install prefix=%{buildroot}/temp

%files
/temp

Summary:        PL/Java for Greenplum Database 
License:        BSD        
Name:           pljava-hawq
Version:        %{pljava_ver}
Release:        %{pljava_rel}
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
BuildArch:      %{buildarch} 
Provides:       pljava = %{pljava_ver}

%description
The PL/Java package provides Procedural language implementation of Java for Greenplum Database. 

%install
mkdir -p %{buildroot}/temp
make -C %{pljava_dir} install prefix=%{buildroot}/temp

%post
echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> $GPHOME/greenplum_path.sh
echo "export LD_LIBRARY_PATH=\$JAVA_HOME/jre/lib/amd64/server/:\$LD_LIBRARY_PATH" >> $GPHOME/greenplum_path.sh

%postun
sed -i".bk" "s|export PATH=\$JAVA_HOME/bin:\$PATH||g" $GPHOME/greenplum_path.sh
sed -i".bk" "s|export LD_LIBRARY_PATH=\$JAVA_HOME/jre/lib/amd64/server:\$LD_LIBRARY_PATH||g" $GPHOME/greenplum_path.sh
rm -rf $GPHOME/greenplum_path.sh.bk

%files
/temp

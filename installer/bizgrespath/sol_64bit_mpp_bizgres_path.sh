BIZHOME=/usr/local/bizgres
GPHOME=$BIZHOME/MPP_RELEASE_VERSION
JAVA_HOME=/usr/local/bizgres/jdk1.5.0_05
PATH=$GPHOME/bin:$GPHOME/gpMgmt/bin:$BIZHOME/client/loader/bin:$JAVA_HOME/bin/amd64:$BIZHOME/gnu/bin:$PATH
LD_LIBRARY_PATH=$GPHOME/lib
MANPATH=$BIZHOME/doc:$MANPATH

export BIZHOME
export GPHOME
export JAVA_HOME
export PATH
export LD_LIBRARY_PATH
export MANPATH

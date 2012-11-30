BIZHOME=/usr/local/bizgres
JAVA_HOME=/usr/local/bizgres/jdk1.5.0_05
PATH=$BIZHOME/pgsql/bin:$BIZHOME/client/loader/bin:$JAVA_HOME/bin/amd64:$BIZHOME/gnu/bin:$PATH
LD_LIBRARY_PATH=$BIZHOME/pgsql/lib:$BIZHOME/gnu/lib
MANPATH=$BIZHOME/doc:$MANPATH

export BIZHOME
export JAVA_HOME
export PATH
export LD_LIBRARY_PATH
export MANPATH

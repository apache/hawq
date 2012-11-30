BIZHOME=/usr/local/bizgres
PATH=$BIZHOME/pgsql/bin:$BIZHOME/client/loader/bin:$PATH
DYLD_LIBRARY_PATH=$BIZHOME/pgsql/lib
MANPATH=$BIZHOME/doc:$MANPATH

export BIZHOME
export PATH
export DYLD_LIBRARY_PATH
export MANPATH

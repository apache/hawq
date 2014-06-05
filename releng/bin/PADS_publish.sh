#!/bin/bash
## ======================================================================

ARTIFACTS=artifacts
PADS_PUBLISH_LOCATION="build@hdsh129.lss.emc.com:/data/downloads/PHD/testing/"

NUMPADS=`find $ARTIFACTS -regex '.*\/PADS-[0-9\.]*-[0-9]*\.tar\.gz' | wc -l | grep -o '[0-9]*'`
if [ "x$NUMPADS" != "x1" ]; then
	echo Found $NUMPADS PADS in $ARTIFACTS, quitting...
	exit 1
fi

NUMPADS=`find $ARTIFACTS -regex '.*\/PADS-[0-9\.]*-bin-[0-9]*\.tar\.gz' | wc -l | grep -o '[0-9]*'`
if [ "x$NUMPADS" != "x1" ]; then
	echo Found $NUMPADS PADS in $ARTIFACTS, quitting...
	exit 1
fi

PADS_TAR=`find $ARTIFACTS -regex '.*\/PADS-[0-9\.]*-[0-9]*\.tar\.gz'`
PADS_BIN_TAR=`find $ARTIFACTS -regex '.*\/PADS-[0-9\.]*-bin-[0-9]*\.tar\.gz'`


cat <<-EOF
	======================================================================
	TIMESTAMP ........ : $( date )
	PADS_TAR ......... : ${PADS_TAR}
	PADS_BIN_TAR ..... : ${PADS_BIN_TAR}
EOF

# TODO: should it publish to 
# build@build-prod.sanmateo.greenplum.com:/var/www/html/internal-builds/greenplum-hawq/rc/${PADS_VERSION}-${BUILD_NUMBER}/
#
# TODO: Should it publish rpm/${PLR_TAR} and rpm/${PGCRYPTO_TAR}
#

PADS_TAR_MD5=$( openssl dgst -md5 ${PADS_TAR} )
PADS_BIN_TAR_MD5=$( openssl dgst -md5 ${PADS_BIN_TAR} )

cat <<-EOF

	Destination(s):
	  ${PADS_PUBLISH_LOCATION}
	======================================================================

	----------------------------------------------------------------------
	Shipping out files:
	  scp -o StrictHostKeyChecking=no ${PADS_TAR} ${PADS_BIN_TAR} ${PADS_PUBLISH_LOCATION}
	----------------------------------------------------------------------

EOF

scp -o StrictHostKeyChecking=no ${PADS_TAR} ${PADS_BIN_TAR} ${PADS_PUBLISH_LOCATION}
if [ $? != 0 ]; then
    echo "FATAL: scp failed (${PADS_PUBLISH_LOCATION})"
    exit 1
fi


cat <<-EOF
	
	----------------------------------------------------------------------
	file(s):
	$( ls -al ${PADS_TAR} ${PADS_BIN_TAR} )

	PADS_TAR_MD5 ....... : ${PADS_TAR_MD5}
	PADS_BIN_TAR_MD5 ... : ${PADS_BIN_TAR_MD5}
	======================================================================
EOF

exit 0

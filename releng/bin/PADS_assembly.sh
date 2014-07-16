#!/bin/bash
## ======================================================================

PULSE_BUILD_NUMBER=${PULSE_BUILD_NUMBER:=9999}
PADS_PUBLISH_LOCATION="build@dist.dh.greenplum.com:/data/dist/PHD/testing/"

pushd src

BUILD_NUMBER=${PULSE_BUILD_NUMBER}

ls hawq*.rpm > /dev/null
if [ $? = 0 ]; then
	HAWQ_RPM=$( ls hawq*.rpm )
else
	echo "HAWQ rpm artifact does not exist."
	exit 2
fi

ls hawq*.tar.gz > /dev/null
if [ $? = 0 ]; then
	HAWQ_TARBALL=$( ls hawq*.tar.gz )
else
	echo "HAWQ tarball artifact does not exist."
	exit 2
fi

ls pxf/build/distributions/pxf*.rpm > /dev/null
if [ $? = 0 ]; then
	PXF_RPM=$( ls pxf/build/distributions/pxf*.rpm )
else
	echo "PXF rpm artifacts do not exist."
	exit 2
fi

ls pxf/build/distributions/pxf*.tar.gz > /dev/null
if [ $? = 0 ]; then
	PXF_TARBALL=$( ls pxf/build/distributions/pxf*.tar.gz )
else
	echo "PXF tarball artifacts do not exist."
	exit 2
fi

wget -nv http://build-prod.dh.greenplum.com/releases/tcServer/2.9.5/vfabric-tc-server-standard-2.9.5.SR1.tar.gz
ls vfabric-tc-server-standard*.tar.gz > /dev/null
if [ $? = 0 ]; then
	TCSERVER_TARBALL=$( ls vfabric-tc-server-standard*.tar.gz )
else
	echo "Failed downloading tcServer tarball."
	exit 2
fi

wget -nv http://build-prod.dh.greenplum.com/releases/tcServer/2.9.5/vfabric-tc-server-standard-2.9.5-SR1.noarch.rpm
ls vfabric-tc-server-standard*.rpm > /dev/null
if [ $? = 0 ]; then
	TCSERVER_RPM=$( ls vfabric-tc-server-standard*.rpm )
else
	echo "Failed downloading tcServer rpm."
	exit 2
fi

PADS_VERSION=$( echo ${HAWQ_RPM} | sed -e 's/hawq-\([0-9]\.[0-9]\.[0-9]\.[0-9]\).*/\1/' )

PADS_TAR=PADS-${PADS_VERSION}-${BUILD_NUMBER}.tar.gz

cat <<-EOF
	======================================================================
	TIMESTAMP ........ : $( date )
	PADS_VERSION ..... : ${PADS_VERSION}
	PADS_TAR ......... : ${PADS_TAR}

----------------------------------------------------------------------
Creating tarball: ${PADS_TAR}
----------------------------------------------------------------------

EOF

rm -rf PADS-${PADS_VERSION}-${BUILD_NUMBER}

mkdir PADS-${PADS_VERSION}-${BUILD_NUMBER}

cp ${HAWQ_RPM} ${PXF_RPM} ${TCSERVER_RPM} PADS-${PADS_VERSION}-${BUILD_NUMBER}

tar zcvf ../${PADS_TAR} PADS-${PADS_VERSION}-${BUILD_NUMBER}
if [ $? != 0 ]; then
    echo "FATAL: tar failed"
    exit 1
fi

popd

PADS_TAR_MD5=$( openssl dgst -md5 ${PADS_TAR} )

cat <<-EOF
	
	----------------------------------------------------------------------
	file(s):
	$( ls -al ${PADS_TAR} ${PADS_BIN_TAR} )

	PADS_TAR_MD5 ....... : ${PADS_TAR_MD5}
	======================================================================
EOF

##
## Publish artifacts?
##

if [ "${PUBLISH_HAWQ_ARTIFACTS}" = "true" ] && ( [[ "${PULSE_PROJECT}"  =~ ^HAWQ-[\.0-9X]+-opt$ ]] || [[ "${PULSE_PROJECT}"  =~ ^HAWQ-main-opt$ ]] ); then 

    GPPKGS=$( ls src/*${BUILD_NUMBER}*.gppkg )

	cat <<-EOF
		======================================================================
		TIMESTAMP ........ : $( date )
		PADS_TAR ......... : ${PADS_TAR}
		GPPKGS ........... : ${GPPKGS}
	
		Destination(s):
		  ${PADS_PUBLISH_LOCATION}
		======================================================================
	
		----------------------------------------------------------------------
		Shipping out files:
		  scp -o StrictHostKeyChecking=no ${PADS_TAR} ${GPPKGS} ${PADS_PUBLISH_LOCATION}
		----------------------------------------------------------------------
	
	EOF
	
	scp -o StrictHostKeyChecking=no ${PADS_TAR} ${GPPKGS} ${PADS_PUBLISH_LOCATION}
	if [ $? != 0 ]; then
	    echo "FATAL: scp failed (${PADS_PUBLISH_LOCATION})"
	    exit 1
	fi
else
    echo "Publishing is disabled ({BLDWRAP_PUBLISH_ARTIFACTS})"
fi

exit 0

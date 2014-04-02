#!/bin/bash
## ======================================================================

PACKAGE_RC=${PACKAGE_RC:=false}
PULSE_BUILD_NUMBER=${PULSE_BUILD_NUMBER:=9999}

if [ "${PACKAGE_RC}" = "true" ]; then
    pushd rpm

    BUILD_NUMBER=${PULSE_BUILD_NUMBER}

    if [ ! -f hawq*.rpm ] && [ ! -f hawq*.tar.gz ]; then
        echo "HAWQ rpm does not exist."
        exit 2
    else
        HAWQ_RPM=$( ls hawq*.rpm )
        HAWQ_TARBALL=$( ls hawq*.tar.gz )
    fi

    if [ ! -f pxf*.rpm ] && [ ! -f pxf*.tar.gz ]; then
        echo "PXF rpm/tarball artifacts do not exist."
        exit 2
    else
        PXF_RPM=$( pxf*.rpm )
        PXF_TARBALL=$( pxf*.tar.gzrpm )
    fi
else
    pushd src

    BUILD_NUMBER=dev

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

    ls pxf/build/pxf*.rpm > /dev/null
    if [ $? = 0 ]; then
        PXF_RPM=$( ls pxf/build/pxf*.rpm )
    else
        echo "PXF rpm artifact does not exist."
        exit 2
    fi

    ls pxf/build/pxf*.tar.gz > /dev/null
    if [ $? = 0 ]; then
        PXF_TARBALL=$( ls pxf/build/pxf*.tar.gz )
    else
        echo "PXF tarball artifact does not exist."
        exit 2
    fi
fi

PADS_VERSION=$( echo ${HAWQ_RPM} | sed -e 's/hawq-\([0-9]\.[0-9]\.[0-9]\).*/\1/' )

PADS_TAR=PADS-${PADS_VERSION}-${BUILD_NUMBER}.tar.gz
PADS_BIN_TAR=PADS-${PADS_VERSION}-bin-${BUILD_NUMBER}.tar.gz
PADS_PUBLISH_LOCATION="build@hdsh129.lss.emc.com:/data/downloads/PHD/latest/"

cat <<-EOF
	======================================================================
	TIMESTAMP ........ : $( date )
	PADS_VERSION ..... : ${PADS_VERSION}
	PADS_TAR ......... : ${PADS_TAR}
	PADS_BIN_TAR ..... : ${PADS_BIN_TAR}
EOF

if [ "${PACKAGE_RC}" = "true" ]; then
	cat <<-EOF
	
		Destination(s):
		  ${PADS_PUBLISH_LOCATION}
		  build@build-prod.sanmateo.greenplum.com:/var/www/html/internal-builds/greenplum-hawq/rc/${PADS_VERSION}-${BUILD_NUMBER}/
		======================================================================
		
	EOF
else
	cat <<-EOF
		======================================================================
		
	EOF
fi

cat <<-EOF

----------------------------------------------------------------------
Creating tarball: ${PADS_TAR}
----------------------------------------------------------------------

EOF

rm -rf PADS-${PADS_VERSION}-${BUILD_NUMBER}

mkdir PADS-${PADS_VERSION}-${BUILD_NUMBER}

cp ${HAWQ_RPM} ${PXF_RPM} PADS-${PADS_VERSION}-${BUILD_NUMBER}

tar zcvf ../${PADS_TAR} PADS-${PADS_VERSION}-${BUILD_NUMBER}
if [ $? != 0 ]; then
    echo "FATAL: tar failed"
    exit 1
fi

rm -f PADS-${PADS_VERSION}-${BUILD_NUMBER}/*

cat <<-EOF

----------------------------------------------------------------------
Creating tarball: ${PADS_BIN_TAR}
----------------------------------------------------------------------

EOF

cp ${HAWQ_TARBALL} ${PXF_TARBALL} PADS-${PADS_VERSION}-${BUILD_NUMBER}

tar zcvf ../${PADS_BIN_TAR} PADS-${PADS_VERSION}-${BUILD_NUMBER}
if [ $? != 0 ]; then
    echo "FATAL: bin tar failed"
    exit 1
fi

popd

PADS_TAR_MD5=$( openssl dgst -md5 ${PADS_TAR} )
PADS_BIN_TAR_MD5=$( openssl dgst -md5 ${PADS_BIN_TAR} )

if [ "${PACKAGE_RC}" != "true" ]; then
    echo "Publishing disabled"
    exit 0
fi

cat <<-EOF

	----------------------------------------------------------------------
	Shipping out files:
	  rsync -auv ${PADS_TAR} ${PADS_BIN_TAR} ${PADS_PUBLISH_LOCATION}
	----------------------------------------------------------------------

EOF

rsync -auv ${PADS_TAR} ${PADS_BIN_TAR} ${PADS_PUBLISH_LOCATION}
if [ $? != 0 ]; then
    echo "FATAL: rsync failed (${PADS_PUBLISH_LOCATION})"
    exit 1
fi

cat <<-EOF

	----------------------------------------------------------------------
	Shipping out files:
	  rsync -auv ${PADS_TAR} ${PADS_BIN_TAR} rpm/QAUtils-RHEL5-x86_64.tar.gz rpm/greenplum-support-dev-RHEL5-x86_64.tar.gz build@build-prod.sanmateo.greenplum.com:/var/www/html/internal-builds/greenplum-hawq/rc/${PADS_VERSION}-${BUILD_NUMBER}/
	----------------------------------------------------------------------

EOF

rsync -auv ${PADS_TAR} ${PADS_BIN_TAR} rpm/QAUtils-RHEL5-x86_64.tar.gz rpm/greenplum-support-dev-RHEL5-x86_64.tar.gz build@build-prod.sanmateo.greenplum.com:/var/www/html/internal-builds/greenplum-hawq/rc/${PADS_VERSION}-${BUILD_NUMBER}/
if [ $? != 0 ]; then
    echo "FATAL: rsync failed (build@build-prod.sanmateo.greenplum.com:/var/www/html/internal-builds/greenplum-hawq/rc/${PADS_VERSION}-${BUILD_NUMBER}/)"
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

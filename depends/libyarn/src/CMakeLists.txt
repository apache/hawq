CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

SET(libyarn_VERSION_MAJOR 0)
SET(libyarn_VERSION_MINOR 1)
SET(libyarn_VERSION_PATCH 17)
SET(libyarn_VERSION_STRING "${libyarn_VERSION_MAJOR}.${libyarn_VERSION_MINOR}.${libyarn_VERSION_PATCH}")
SET(libyarn_VERSION_API 1)
SET(libyarn_ROOT_SOURCES_DIR ${CMAKE_SOURCE_DIR}/src)
SET(libyarn_COMMON_SOURCES_DIR ${libyarn_ROOT_SOURCES_DIR}/common)

IF(ENABLE_DEBUG)
	SET(libyarn_VERSION_STRING "${libyarn_VERSION_STRING}d")
ENDIF(ENABLE_DEBUG)

CONFIGURE_FILE(platform.h.in platform.h)
CONFIGURE_FILE(doxyfile.in doxyfile)

AUTO_SOURCES(files "*.cpp" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libyarn_SOURCES ${files})

AUTO_SOURCES(files "*.cc" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libyarn_SOURCES ${files})

AUTO_SOURCES(files "*.c" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libyarn_SOURCES ${files})

AUTO_SOURCES(files "*.h" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
LIST(APPEND libyarn_SOURCES ${files})

AUTO_SOURCES(libyarn_PROTO_FILES "proto/*.proto" "RECURSE" "${CMAKE_CURRENT_SOURCE_DIR}")
SET(libyarn_PROTO_FILES ${libyarn_PROTO_FILES} PARENT_SCOPE)

PROTOBUF_GENERATE_CPP(libyarn_PROTO_SOURCES libyarn_PROTO_HEADERS ${libyarn_PROTO_FILES})

SET(HEADER 
    common/Exception.h
    common/XmlConfig.h
    libyarnclient/ApplicationClient.h
    libyarnclient/ApplicationMaster.h
	libyarnserver/ApplicationClientProtocol.h
	libyarnserver/ApplicationMasterProtocol.h
	protocolrecords/GetApplicationReportRequest.h
    protocolrecords/GetApplicationReportResponse.h
    protocolrecords/GetNewApplicationRequest.h
    protocolrecords/GetNewApplicationResponse.h
    protocolrecords/SubmitApplicationRequest.h
    protocolrecords/SubmitApplicationResponse.h
    protocolrecords/RegisterApplicationMasterRequest.h
    protocolrecords/RegisterApplicationMasterResponse.h
    protocolrecords/FinishApplicationMasterRequest.h
    protocolrecords/FinishApplicationMasterResponse.h
    protocolrecords/AllocateRequest.h
	protocolrecords/AllocateResponse.h
	protocolrecords/StartContainerRequest.h
	protocolrecords/StartContainerResponse.h
	protocolrecords/StartContainersRequest.h
	protocolrecords/StopContainersRequest.h
	protocolrecords/GetClusterNodesRequest.h
	protocolrecords/GetClusterNodesResponse.h
  protocolrecords/GetContainersRequest.h
  protocolrecords/GetContainersResponse.h
	protocolrecords/GetQueueInfoRequest.h
	protocolrecords/GetQueueInfoResponse.h
	protocolrecords/KillApplicationRequest.h
	protocolrecords/KillApplicationResponse.h
	protocolrecords/GetClusterMetricsRequest.h
	protocolrecords/GetClusterMetricsResponse.h
	protocolrecords/GetApplicationsRequest.h
	protocolrecords/GetApplicationsResponse.h
	protocolrecords/GetQueueUserAclsInfoRequest.h
	protocolrecords/GetQueueUserAclsInfoResponse.h
    libyarnclient/LibYarnClient.h
    libyarnclient/LibYarnClientC.h
    libyarnclient/LibYarnConstants.h
    )

SET(RECORDS_HEADER 
   	records/ApplicationAttemptId.h
    records/ApplicationId.h
    records/ApplicationReport.h
    records/ApplicationResourceUsageReport.h
    records/FinalApplicationStatus.h
    records/Resource.h
    records/YarnApplicationState.h
    records/ApplicationSubmissionContext.h
    records/ContainerLaunchContext.h
    records/Priority.h
    records/AMCommand.h
    records/Container.h
    records/ContainerId.h
    records/ContainerStatus.h
    records/ContainerExitStatus.h
    records/ContainerReport.h
    records/NMToken.h
    records/NodeId.h
    records/ResourceBlacklistRequest.h
    records/ResourceRequest.h
    records/NodeReport.h
    records/NodeState.h
    records/FunctionResult.h
    records/PreemptionMessage.h
    records/StrictPreemptionContract.h
    records/PreemptionContract.h
    records/PreemptionResourceRequest.h
    records/PreemptionContainer.h
    records/ContainerState.h
    records/Token.h
    records/StringBytesMap.h
    records/QueueInfo.h
    records/QueueState.h
    records/YarnClusterMetrics.h
    records/QueueUserACLInfo.h
    records/QueueACL.h
   )
      
SET(libyarncommon_HEADER
	libyarncommon/Token.h
	)

ADD_LIBRARY(libyarn-static STATIC ${libyarn_SOURCES} ${libyarn_PROTO_SOURCES} ${libyarn_PROTO_HEADERS})
ADD_LIBRARY(libyarn-shared SHARED ${libyarn_SOURCES} ${libyarn_PROTO_SOURCES} ${libyarn_PROTO_HEADERS})

ADD_CUSTOM_COMMAND(
	TARGET libyarn-shared libyarn-static
	PRE_BUILD
	COMMAND echo ${libyarn_VERSION_STRING} > version
	WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

TARGET_LINK_LIBRARIES(libyarn-static pthread)
TARGET_LINK_LIBRARIES(libyarn-shared pthread)
       
IF(NEED_BOOST)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
    TARGET_LINK_LIBRARIES(libyarn-static boost_thread)
    TARGET_LINK_LIBRARIES(libyarn-static boost_chrono)
    TARGET_LINK_LIBRARIES(libyarn-static boost_system)
    TARGET_LINK_LIBRARIES(libyarn-static boost_atomic)
    TARGET_LINK_LIBRARIES(libyarn-shared boost_thread)
    TARGET_LINK_LIBRARIES(libyarn-shared boost_chrono)
    TARGET_LINK_LIBRARIES(libyarn-shared boost_system)
    TARGET_LINK_LIBRARIES(libyarn-shared boost_atomic)
ENDIF(NEED_BOOST)

IF(NEED_GCCEH)
	TARGET_LINK_LIBRARIES(libyarn-static gcc_eh)
	TARGET_LINK_LIBRARIES(libyarn-shared gcc_eh)
ENDIF(NEED_GCCEH)

IF(OS_LINUX)
    TARGET_LINK_LIBRARIES(libyarn-static uuid)
	TARGET_LINK_LIBRARIES(libyarn-shared uuid)
ENDIF(OS_LINUX)

INCLUDE_DIRECTORIES(${libyarn_ROOT_SOURCES_DIR})
INCLUDE_DIRECTORIES(${libyarn_COMMON_SOURCES_DIR})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${LIBXML2_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${KERBEROS_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${GSASL_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/mock)

TARGET_LINK_LIBRARIES(libyarn-static ${PROTOBUF_LIBRARIES})
TARGET_LINK_LIBRARIES(libyarn-static ${LIBXML2_LIBRARIES})
TARGET_LINK_LIBRARIES(libyarn-static ${KERBEROS_LIBRARIES})
TARGET_LINK_LIBRARIES(libyarn-static ${GSASL_LIBRARIES})

TARGET_LINK_LIBRARIES(libyarn-shared ${PROTOBUF_LIBRARIES})
TARGET_LINK_LIBRARIES(libyarn-shared ${LIBXML2_LIBRARIES})
TARGET_LINK_LIBRARIES(libyarn-shared ${KERBEROS_LIBRARIES})
TARGET_LINK_LIBRARIES(libyarn-shared ${GSASL_LIBRARIES})

SET_TARGET_PROPERTIES(libyarn-static PROPERTIES OUTPUT_NAME "yarn")
SET_TARGET_PROPERTIES(libyarn-shared PROPERTIES OUTPUT_NAME "yarn")

IF(NEED_BOOST)
	SET_TARGET_PROPERTIES(libyarn-shared libyarn-static PROPERTIES LINK_FLAGS "-L${Boost_LIBRARY_DIRS}")
ENDIF(NEED_BOOST)

SET_TARGET_PROPERTIES(libyarn-shared PROPERTIES 
    VERSION ${libyarn_VERSION_MAJOR}.${libyarn_VERSION_MINOR}.${libyarn_VERSION_PATCH} 
    SOVERSION ${libyarn_VERSION_API})

INSTALL(TARGETS libyarn-static libyarn-shared
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
INSTALL(FILES ${HEADER} DESTINATION include/libyarn)
INSTALL(FILES ${RECORDS_HEADER} DESTINATION include/libyarn/records)
INSTALL(FILES ${libyarn_PROTO_HEADERS} DESTINATION include/libyarn/records)
INSTALL(FILES ${libyarncommon_HEADER} DESTINATION include/libyarn/libyarncommon)
            
SET(libyarn_SOURCES ${libyarn_SOURCES} PARENT_SCOPE)
SET(libyarn_PLATFORM_HEADER_DIR ${CMAKE_CURRENT_BINARY_DIR} PARENT_SCOPE)
SET(libyarn_ROOT_SOURCES_DIR ${libyarn_ROOT_SOURCES_DIR} PARENT_SCOPE)
SET(libyarn_COMMON_SOURCES_DIR ${libyarn_COMMON_SOURCES_DIR} PARENT_SCOPE)

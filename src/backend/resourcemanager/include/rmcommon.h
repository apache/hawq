#ifndef HAWQ_RESOURCEMANAGER_COMMON_DEFINITIONS
#define HAWQ_RESOURCEMANAGER_COMMON_DEFINITIONS

#define RESOURCE_QUEUE_DEFAULT_QUEUE_NAME	"pg_default"
#define RESOURCE_QUEUE_ROOT_QUEUE_NAME		"pg_root"
#define RESOURCE_QUEUE_SEG_RES_QUOTA_MEM	"mem:"
#define RESOURCE_QUEUE_SEG_RES_QUOTA_CORE   "core:"
#define RESOURCE_QUEUE_RATIO_SIZE			32
#define USER_DEFAULT_PRIORITY				3
#define RESOURCE_QUEUE_PARALLEL_COUNT_DEF   100
#define RESOURCE_QUEUE_SEG_RES_QUOTA_DEF	128
#define RESOURCE_QUEUE_RES_UPPER_FACTOR_DEF 2

/* The queue is a valid leaf queue. */
#define RESOURCE_QUEUE_STATUS_VALID_LEAF				0x00000001
#define NOT_RESOURCE_QUEUE_STATUS_VALID_LEAF			0xFFFFFFFE
/* The queue is a valid branch queue. */
#define RESOURCE_QUEUE_STATUS_VALID_BRANCH				0x00000002
#define NOT_RESOURCE_QUEUE_STATUS_VALID_BRANCH			0xFFFFFFFD
/* The queue is in use. */
#define RESOURCE_QUEUE_STATUS_VALID_INUSE				0x00000004
/* The queue is invalid. */
#define RESOURCE_QUEUE_STATUS_VALID_INVALID				0x00000008

#define RESOURCE_QUEUE_STATUS_EXPRESS_PERCENT			0x00000010

/* The queue is the root/default queue node. No parent node any more. */
#define RESOURCE_QUEUE_STATUS_IS_ROOT					0x80000000
#define RESOURCE_QUEUE_STATUS_IS_DEFAULT				0x40000000
#define RESOURCE_QUEUE_STATUS_IS_VER1X					0x20000000

#define RESOURCE_QUEUE_DDL_ATTR_LENGTH_MAX			 	64
#define RESOURCE_QUEUE_DDL_POLICY_LENGTH_MAX			64
#define RESOURCE_ROLE_DDL_ATTR_LENGTH_MAX			 	64

#define LIBPQ_CONNECT_TIMEOUT 							60
/**
 * Structure for expressing resource bundle (mem,core).
 */
struct ResourceBundleData {
	int32_t			MemoryMB;
	double			Core;
	uint32_t		Ratio;
};
typedef struct ResourceBundleData *ResourceBundle;
typedef struct ResourceBundleData  ResourceBundleData;

/*
 * For each allocated container from global resource manager, one following
 * structured instance is created to track the life of this container.
 */
#define RESOURCE_CONTAINER_MAX_LIFETIME 10

/* Maximum slots for specifying resource allocation policies in resource pool */
#define RESOURCEPOOL_MAX_ALLOC_POLICY_SIZE 10

/* Timeout of ms as poll() and select() argument for communication. */
#define RESOURCE_NETWORK_POLL_TIMEOUT 100

#endif /* HAWQ_RESOURCEMANAGER_COMMON_DEFINITIONS */

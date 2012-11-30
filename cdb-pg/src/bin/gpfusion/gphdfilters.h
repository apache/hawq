/*
 * gphdfilters.h
 *
 * Header for handling push down of supported scan level filters to hadoop.
 * 
 * Copyright (c) 2012, Greenplum inc
 */
#ifndef _GPHDFS_GPHDFILTERS_H_
#define _GPHDFS_GPHDFILTERS_H_

#include "postgres.h"
#include "access/attnum.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"

/*
 * each supported operator has a code that will describe the operator
 * type in the final serialized string that gets pushed down.
 *
 * NOTE: the codes could be forced into a single byte, but list will
 * grow larger in the future, so why bother.
 */
typedef enum GPHDOperatorCode
{
	HDOP_LT = 1,
	HDOP_GT,
	HDOP_LE,
	HDOP_GE,
	HDOP_EQ,
	HDOP_NE,
	HDOP_AND

} GPHDOperatorCode;

/*
 * each supported operand from both sides of the operator is represented
 * by a code that will describe the operator type in the final serialized
 * string that gets pushed down.
 */
#define GPHD_ATTR_CODE		'a'
#define GPHD_CONST_CODE		'c'
#define GPHD_OPERATOR_CODE	'o'

/*
 * An Operand has any of the above codes, and the information specific to
 * its type. This could be compacted but filter structures are expected to
 * be very few and very small so it's ok as is.
 */
typedef struct GPHDOperand
{
	char		opcode;		/* GPHD_ATTR_CODE or GPHD_CONST_CODE*/
	AttrNumber 	attnum;		/* used when opcode is GPHD_ATTR_CODE */
	StringInfo 	conststr;	/* used when opcode is GPHD_CONST_CODE */

} GPHDOperand;

/*
 * A GPHD filter has a left and right operands, and an operator.
 */
typedef struct GPHDFilterDesc
{
	GPHDOperand 		l;		/* left operand	*/
	GPHDOperand 		r;		/* right operand or InvalidAttrNumber if none */
	GPHDOperatorCode	op;		/* operator	code */

} GPHDFilterDesc;

/*
 * GP operator OID to GPHD operator code mapping
 */
typedef struct gpop_hdop_map
{
	Oid					gpop;
	GPHDOperatorCode	hdop;

} gpop_hdop_map;

static inline bool gphdoperand_is_attr(GPHDOperand x)
{
	return (x.opcode == GPHD_ATTR_CODE);
}

static inline bool gphdoperand_is_const(GPHDOperand x)
{
	return (x.opcode == GPHD_CONST_CODE);
}

char *serializeGPHDFilterQuals(List *quals);

#endif

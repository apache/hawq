/*
 * atomic.c
 *    routines for atomic operations.
 *
 * Copyright (c) 2011 - present, EMC DCD (Greenplum)
 */

#include "postgres.h"
#include "utils/atomic.h"

#if defined(_AIX)
#include <sys/atomic_op.h>
#elif defined(__sparc__)
#include <atomic.h>
#endif

/*
 * compare_and_swap_32
 *   Atomic compare and swap operation in a 32-bit address.
 *
 * Algorithm:
 * 		if (*dest == old)
 * 		{
 * 			*dest = new;
 * 			return 1;
 * 		}
 * 		return 0;
 *
 */
int32 compare_and_swap_32(uint32 *dest, uint32 old, uint32 new)
{
	if (old == new)
		return true;

	volatile int32 res = 0;

#if defined(__i386)
	__asm__ __volatile__
		(
		"MOVL 0x8(%%ebp), %%edx 						\n\t"
		"MOVL 0xc(%%ebp), %%eax			 				\n\t"
		"MOVL 0x10(%%ebp), %%ecx 						\n\t"
		"LOCK CMPXCHG %%ecx, (%%edx)			\n\t"
		"SETE %0								\n\t"
		:"=g"(res)
		:
		:"%eax","%ecx","%edx"
		);

#elif defined(__x86_64__)
	res = __sync_bool_compare_and_swap(dest, old, new);

#elif defined(__sparc__)
	uint32 returnValue = atomic_cas_32(dest, old, new);
	res = !(returnValue == *dest);

#else
#error unsupported platform: requires atomic compare_and_swap_32 operation
#endif

	return res;
}

/*
 * compare_and_swap_64
 *   Atomic compare and swap operation in a 64-bit address.
 *
 * The algorithm is similar to compare_and_swap_32.
 */
int32 compare_and_swap_64(uint64 *dest, uint64 old, uint64 new)
{
	if (old == new)
		return true;

	volatile int32 res = 0;
	
#if defined(__x86_64__)
	res = __sync_bool_compare_and_swap(dest, old, new);

#elif defined(__sparc__)
	uint64 returnValue = atomic_cas_64(dest, old, new);
	res = !(returnValue == *dest);

#elif defined(_AIX)
	res = compare_and_swap(dest, &old, new);

#elif !defined(__i386)
#error unsupported platform: requires atomic compare_and_swap_64 operation
#endif

	/* All others, return 0 */
	return res;
}

/*
 * compare_and_swap_ulong
 *   a generic cas operation for an unsigned long value in both
 *   32-bit and 64-bit platform.
 */
int32 compare_and_swap_ulong(unsigned long *dest,
							 unsigned long oldval,
							 unsigned long newval)
{
	int32 casResult = false;

#if defined(__i386)
	Assert(sizeof(unsigned long) == sizeof(uint32));
	casResult = compare_and_swap_32((uint32*)dest,
									(uint32)oldval,
									(uint32)newval);
#else
	if (sizeof(unsigned long) == sizeof(uint64))
	{
		casResult = compare_and_swap_64((uint64*)dest,
										(uint64)oldval,
										(uint64)newval);
	}
	
	else
	{
		Assert(sizeof(unsigned long) == sizeof(uint32));
		casResult = compare_and_swap_32((uint32*)dest,
										(uint32)oldval,
										(uint32)newval);
	}
#endif

	return casResult;
}
	
/* 
 * gp_atomic_add_32
 *    Atomic increment in a 32-bit address, and return the incremented value.
 */
int32 gp_atomic_add_32(volatile int32 *ptr, int32 inc)
{
	volatile int32 newValue = 0;
	
#if defined(__x86_64__)
	int32 oldValue = __sync_fetch_and_add(ptr, inc);
	newValue = oldValue + inc;

#elif defined(__i386)
	__asm__ __volatile__ (
		"lock; xaddl %0, %1"
		: "=r" (newValue), "=m" (*ptr)
		: "0" (inc), "m" (*ptr)
		: "memory");

	newValue += inc;

#elif defined(__sparc__)
	newValue = atomic_add_32_nv((volatile uint32_t *)ptr, inc);

#else
#error unsupported platform: requires atomic_add_32 operation
#endif

	return newValue;
}

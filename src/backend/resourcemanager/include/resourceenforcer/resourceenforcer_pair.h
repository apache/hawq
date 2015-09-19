#ifndef _HAWQ_RESOURCEENFORCER_PAIR_H
#define _HAWQ_RESOURCEENFORCER_PAIR_H

struct PairData
{
	void	*Key;
	void	*Value;
};

typedef struct PairData *Pair;
typedef struct PairData  PairData;

Pair createPair(void *key, void *value);
void freePair(Pair p);

#endif /* _HAWQ_RESOURCEENFORCER_PAIR_H */

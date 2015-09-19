/*
 * NodeState.h
 *
 *  Created on: Jul 18, 2014
 *      Author: bwang
 */

#ifndef NODESTATE_H_
#define NODESTATE_H_

enum NodeState {
  NS_NEW = 1,
  NS_RUNNING = 2,
  NS_UNHEALTHY = 3,
  NS_DECOMMISSIONED = 4,
  NS_LOST = 5,
  NS_REBOOTED = 6
};

#endif /* NODESTATE_H_ */

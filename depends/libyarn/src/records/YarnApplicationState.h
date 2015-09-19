/*
 * YarnApplicationState.h
 *
 *  Created on: Jul 9, 2014
 *      Author: bwang
 */

#ifndef YARNAPPLICATIONSTATE_H_
#define YARNAPPLICATIONSTATE_H_

enum YarnApplicationState {
  NEW = 1,
  NEW_SAVING = 2,
  SUBMITTED = 3,
  ACCEPTED = 4,
  RUNNING = 5,
  FINISHED = 6,
  FAILED = 7,
  KILLED = 8
};


#endif /* YARNAPPLICATIONSTATE_H_ */

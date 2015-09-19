/*
 * ContainerExitStatus.h
 *
 *  Created on: Jan 5, 2015
 *      Author: weikui
 */

#ifndef CONTAINEREXITSTATUS_H_
#define CONTAINEREXITSTATUS_H_

enum ContainerExitStatus {
	SUCCESS = 0,
	ABORTED = -100,
	DISKS_FAILED = -101,
	INVALID = -1000
};
#endif /* CONTAINEREXITSTATUS_H_ */

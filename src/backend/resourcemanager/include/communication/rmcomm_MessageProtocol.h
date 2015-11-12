/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MESSAGEPROTO_H
#define RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MESSAGEPROTO_H

/*
 * BASIC socket communication data format.
 *
 * Receive the message content and save the request into the connection track
 * instance for processing later. The message format:
 *
 * Format: ( 8-byte aligned memory block. )
 *
 *         |<----------- 64 bits (8 bytes) ----------->|
 *         ---------------------------------------------
 *         |    M    S    G    S    T    A    R    T   |
 *         +---------------------+---------------------+
 *         | Marks(2B) | ID (2B) |     Length (4B)     |
 * 		   +---------------------+---------------------+
 *         |                                           |
 *         |              Message (n * 8B)             |
 *         |                                           |
 *         +-------------------------------------------+
 *		   |    M    S    G    E    N    D    S    !   |
 *		   ---------------------------------------------
 *
 *        |<-------- mark1------->|<--------mark2-------->|
 * Marks: +-----------------------+-----------------------+
 *        | 0| 1| 2| 3| 4| 5| 6| 7| 8| 9|10|11|12|13|14|15|
 *        +-----------------------+-----------------------+
 **/

struct RMMessageHeadData
{
	char 		TAG[8];
	uint8_t		Mark1;
	uint8_t		Mark2;
	uint16_t	MessageID;
	uint32_t	MessageSize;
};

struct RMMessageTailData
{
	char		TAG[8];
};

typedef struct RMMessageHeadData    RMMessageHeadData;
typedef struct RMMessageHeadData   *RMMessageHead;
typedef struct RMMessageTailData    RMMessageTailData;
typedef struct RMMessageTailData   *RMMessageTail;

#define DRM_MSGFRAME_HEADTAG_STR			"MSGSTART"
#define DRM_MSGFRAME_TAILTAG_STR			"MSGENDS!"

#define DRM_MSGFRAME_HEADTAGSIZE	   (sizeof(DRM_MSGFRAME_HEADTAG_STR) -1)
#define DRM_MSGFRAME_HEADSIZE		   (sizeof(RMMessageHeadData))
#define DRM_MSGFRAME_TAILSIZE		   (sizeof(RMMessageTailData))
#define DRM_MSGFRAME_TOTALSIZE(h)	   ((h)->MessageSize +					   \
										DRM_MSGFRAME_HEADSIZE + 			   \
										DRM_MSGFRAME_TAILSIZE)
#define DRM_MSGFRAME_HEADTAG_MATCHED(h)										   \
				(h[0] == 'M' && h[1] == 'S' && h[2] == 'G' && h[3] == 'S' &&   \
				 h[4] == 'T' && h[5] == 'A' && h[6] == 'R' && h[7] == 'T')
#define DRM_MSGFRAME_TAILTAG_MATCHED(t)										   \
				(t[0] == 'M' && t[1] == 'S' && t[2] == 'G' && t[3] == 'E' &&   \
				 t[4] == 'N' && t[5] == 'D' && t[6] == 'S' && t[7] == '!')

#endif /* RESOURCE_MAMANGER_INTER_PROCESS_COMMUNICATION_MESSAGEPROTO_H */

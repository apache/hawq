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

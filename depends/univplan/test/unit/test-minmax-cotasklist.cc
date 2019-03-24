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

#include <errno.h>

#include "gtest/gtest.h"

#include "dbcommon/log/exception.h"
#include "dbcommon/log/logger.h"

#include "univplan/minmax/minmax-predicates.h"

namespace univplan {
//-----------------------------------------------------------------------------
// 1 source task covers target task first half
//-----------------------------------------------------------------------------

TEST(TestMinMaxCOBlockTask, BasicIntersect11) {
  // task        [[           ]]
  // src   [[  ]       ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 1024, 128, 100);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(1024, task1.taskBeginRowId);
  EXPECT_EQ(1279, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicIntersect12) {
  // task        [[                ]]
  // src   [[         ]       ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 1024, 228, 100);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(256, task1.taskBeginRowId);
  EXPECT_EQ(327, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicIntersect13) {
  // task        [[               ]]
  // src   [             [  ] ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 1024, 512, 100);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(512, task1.taskBeginRowId);
  EXPECT_EQ(611, task1.getTaskEndRowId());
}

//-----------------------------------------------------------------------------
// 2 source task covers target task completely
//-----------------------------------------------------------------------------

TEST(TestMinMaxCOBlockTask, BasicIntersect21) {
  // task        [[           ]]
  // src   [[  ]                  ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 2048, 128, 100);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(0, task1.taskRowCount);
}

TEST(TestMinMaxCOBlockTask, BasicIntersect22) {
  // task     [[           ]]
  // src   [ [   ]                 ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 2048, 128, 256);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(256, task1.taskBeginRowId);
  EXPECT_EQ(383, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicIntersect23) {
  // task        [[           ]]
  // src   [             [  ]           ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 2048, 512, 100);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(512, task1.taskBeginRowId);
  EXPECT_EQ(611, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicIntersect24) {
  // task        [[           ]]
  // src   [                [    ]  ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 2048, 1024, 512);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(1024, task1.taskBeginRowId);
  EXPECT_EQ(1279, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicIntersect25) {
  // task        [[           ]]
  // src   [                     [  ]  ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 2048, 1536, 100);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(0, task1.taskRowCount);
}

TEST(TestMinMaxCOBlockTask, BasicIntersect26) {
  // task        [[           ]]
  // src   [   [                  ]  ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 2048, 128, 1536);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(256, task1.taskBeginRowId);
  EXPECT_EQ(1279, task1.getTaskEndRowId());
}

//-----------------------------------------------------------------------------
// 3 source task covers target task second half
//-----------------------------------------------------------------------------

TEST(TestMinMaxCOBlockTask, BasicIntersect31) {
  // task        [[           ]]
  // src             [   [  ]           ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 512, 2048, 513, 100);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(513, task1.taskBeginRowId);
  EXPECT_EQ(612, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicIntersect32) {
  // task     [[           ]]
  // src              [ [       ]    ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 512, 2048, 513, 1024);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(513, task1.taskBeginRowId);
  EXPECT_EQ(1279, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicIntersect33) {
  // task        [[           ]]
  // src              [             [  ]   ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 2048, 1536, 100);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(0, task1.taskRowCount);
}

//-----------------------------------------------------------------------------
// 4 target task covers source task
//-----------------------------------------------------------------------------

TEST(TestMinMaxCOBlockTask, BasicIntersect41) {
  // task        [[             ]]
  // src            [ [    ]  ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 512, 512, 520, 100);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(520, task1.taskBeginRowId);
  EXPECT_EQ(619, task1.getTaskEndRowId());
}

//----------------------------------------
// 5 second source task after intersected
//----------------------------------------

TEST(TestMinMaxCOBlockTask, BasicIntersect51) {
  // task        [[             ]]
  // src            [ [    ] ][[    ]]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 512, 512, 520, 100);
  COBlockTask tasksrc2(1280, 1024, 512);
  task1.resetIntersection();
  task1.intersect(tasksrc);
  EXPECT_EQ(520, task1.taskBeginRowId);
  EXPECT_EQ(619, task1.getTaskEndRowId());
  task1.intersect(tasksrc2);
  EXPECT_EQ(520, task1.taskBeginRowId);
  EXPECT_EQ(1279, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicUnionAll11) {
  // task        [[           ]]
  // src   [[  ]       ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 1024, 128, 100);
  task1.resetUnionAll();
  task1.unionAll(tasksrc);
  EXPECT_EQ(256, task1.taskBeginRowId);
  EXPECT_EQ(1279, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicUnionAll12) {
  // task        [[           ]]
  // src      [ [     ]  ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 1024, 128, 512);
  task1.resetUnionAll();
  task1.unionAll(tasksrc);
  EXPECT_EQ(256, task1.taskBeginRowId);
  EXPECT_EQ(1279, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicUnionAll13) {
  // task        [[                 ]]
  // src               [ [     ]  ]
  COBlockTask task1(1024, 256, 2048);
  COBlockTask tasksrc(256, 512, 512);
  task1.resetUnionAll();
  task1.unionAll(tasksrc);
  EXPECT_EQ(256, task1.taskBeginRowId);
  EXPECT_EQ(2303, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicUnionAll14) {
  // task        [[                 ]]
  // src               [ [               ]  ]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 1024, 512);
  task1.resetUnionAll();
  task1.unionAll(tasksrc);
  EXPECT_EQ(256, task1.taskBeginRowId);
  EXPECT_EQ(1279, task1.getTaskEndRowId());
}

TEST(TestMinMaxCOBlockTask, BasicUnionAll21) {
  // task        [[       ]]
  // src     [[               ]]
  COBlockTask task1(1024, 256, 1024);
  COBlockTask tasksrc(256, 0, 2048);
  task1.resetUnionAll();
  task1.unionAll(tasksrc);
  EXPECT_EQ(256, task1.taskBeginRowId);
  EXPECT_EQ(1279, task1.getTaskEndRowId());
}
}  // namespace univplan

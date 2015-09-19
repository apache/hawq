/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "gtest/gtest.h"

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
#ifdef DATA_DIR
    chdir(DATA_DIR);
#endif
    return RUN_ALL_TESTS();
}

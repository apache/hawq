libyarn    a c/c++ YARN client
========================

1 Requirement

To build libyarn, the following libraries are needed.

    cmake (2.8+)                    http://www.cmake.org/
    boost (tested on 1.53+)         http://www.boost.org/
    google protobuf                 http://code.google.com/p/protobuf/
    libxml2                         http://www.xmlsoft.org/
    kerberos                        http://web.mit.edu/kerberos/
    libuuid                         http://sourceforge.net/projects/libuuid/

To run tests, the following library is needed.

    gtest (tested on 1.7.0)         already integrated in the source code
    gmock (tested on 1.7.0)         already integrated in the source code

To run code coverage test, the following tools are needed.

    gcov (included in gcc distribution)
    lcov (tested on 1.9)            http://ltp.sourceforge.net/coverage/lcov.php

2 Configuration

Assume libyarn home directory is LIBYARN_HOME.

    cd LIBYARN_HOME
    mkdir build
    cd build
    ../bootstrap

Environment variable CC and CXX can be used to setup the compiler.
Script "bootstrap" is basically a wrapper of cmake command, user can use cmake directly to turn the configuration. 

Run command "../bootstrap --help" for more configuration. 

3 Build

Run command to build
    
    make
    
To build concurrently, rum make with -j option.

    make -j8

4 Test

To do unit test, run command

    make unittest
    
To do function test, first start YARN, and create the function test configure file at LIBYARN_HOME/test/data/function-test.xml, an example can be found at LIBYARN_HOME/test/data/function-test.xml.example. And run command.

    make functiontest
    
To show code coverage result, run command. Code coverage result can be found at BUILD_DIR/CodeCoverageReport/index.html

    make ShowCoverage

5 Install

To install libyarn, run command

    make install

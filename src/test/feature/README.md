This folder contains the feature tests of apache HAWQ. The feature test library is written based on google test. Users and developers could run these tests after successfully installed apache HAWQ.

# Build 
`cd $HAWQ_HOME`

`make feature-test`

`make feature-test-clean`

Also, you could enter the `$HAWQ_HOME/src/test/feature` folder to `make` and `make clean`. 

Before building the code of feature tests part, just make sure your compiler supports C++11 and you have apache HAWQ successfully compiled.

# Run
1. Make sure HAWQ is running correctly. If not, `init` or `start` HAWQ at first.
2. Load environment configuration by running `source $INSTALL_PREFIX/greenplum_path.sh`.
3. Load hdfs configuration. For example, `export HADOOP_HOME=/Users/wuhong/hadoop-2.7.2 && export PATH=${PATH}:${HADOOP_HOME}/bin`. Since some test cases need `hdfs` and `hadoop` command, just ensure these commands work before running. Otherwise you will get failure.
4. Run the cases with`./parallel-run-feature-test.sh 8 ./feature-test`(in this case 8 threads in parallel), you could use `--gtest_filter` option to filter test cases(both positive and negative patterns are supported). Please see more options by running `./feature-test --help`.

# Development
In contribution to HAWQ, we suggest developers submitting feature tests related to your feature development. In writting a featurte test, you need to write a cpp file inside corresponding folders. There are two recommended way to write this cpp file:

1. Check every single sql clause and bundle the test utility inside one case. This way is not that readable but it is much better in quality. See `ExternalSource/test_errortbl.cpp` for example. 
2. Write a `sql` file for sql utility test related to your feature and a `.ans` which lists the expected output. Lots of current cases are written in this manner. For sql plan cases, you should include two sperate sql files linking to `OCAR` on and off.

Refer to `testlib/test_lib.cpp` for detialed usage. The APIs are listed in `lib/sql_util.h`. 

You don't need to modify `Makefile`, it will automatively handle new .

# Troubleshooting
If some cases failed in your environment, check it out with the generated `.diff` files: `find . -name "*.out.diff"`. There files show the difference between the expected output(`.ans`) and the real output(`.out`) which you could find if seperate subfolders after running.

There are some cases expected to be fail in specific environment which need to be fixed later on. Don't worry about that.

To run feature tests in parallel, make sure your python version is equal to or greater than 2.7.

# Reference
[HAWQ-832](https://issues.apache.org/jira/browse/HAWQ-832)

[Design Doc](https://issues.apache.org/jira/secure/attachment/12811319/GoogleTest.pdf)

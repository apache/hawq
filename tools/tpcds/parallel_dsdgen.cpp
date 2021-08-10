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
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

const std::vector<std::string> tableNames{"call_center",
                                          "catalog_page",
                                          "catalog_returns",
                                          "catalog_sales",
                                          "customer",
                                          "customer_address",
                                          "customer_demographics",
                                          "date_dim",
                                          "household_demographics",
                                          "income_band",
                                          "inventory",
                                          "item",
                                          "promotion",
                                          "reason",
                                          "ship_mode",
                                          "store",
                                          "store_returns",
                                          "store_sales",
                                          "time_dim",
                                          "warehouse",
                                          "web_page",
                                          "web_returns",
                                          "web_sales",
                                          "web_site"};

bool checkFileExist(const std::string& file) {
  std::ifstream inf(file);
  if (inf.is_open()) {
    inf.close();
    return true;
  }
  return false;
}

int parallelGenData(const std::string& gphome, const std::string& scale,
                    const std::string& parallel,
                    std::vector<std::string>& childids,
                    const std::string& dataPath) {
  pid_t childpid;
  int execStatus;
  std::vector<pid_t> childprocess;
  const std::string dsdgen = gphome + "/bin/dsdgen";
  const std::string idxPath = gphome + "/bin/tpcds.idx";
  if (!checkFileExist(dsdgen) || !checkFileExist(idxPath)) {
    std::cerr << "File not exist: " << dsdgen << " or " << idxPath << std::endl;
    return -1;
  }
  for (size_t i = 0; i != childids.size(); ++i) {
    childpid = fork();
    switch (childpid) {
      case -1:
        std::cerr << "fork error!" << std::endl;
        exit(EXIT_FAILURE);
      case 0:
        execStatus =
            execl(dsdgen.c_str(), "dsdgen", "-scale", scale.c_str(),
                  "-distributions", idxPath.c_str(), "-parallel",
                  parallel.c_str(), "-child", childids[i].c_str(), "-dir",
                  dataPath.c_str(), "-terminate", "N", "-force", "Y", NULL);
        if (execStatus < 0) {
          perror("ERROR on execl");
          exit(EXIT_FAILURE);
        }
        exit(EXIT_SUCCESS);
      default:
        childprocess.push_back(childpid);
        break;
    }
  }
  int status;
  int counter = 1;
  sleep(1);
  while (!childprocess.empty()) {
    std::cout << "Waiting child process(pid:" << childprocess.back() << " "
              << counter << "/" << childids.size() << ") done ..." << std::endl;
    pid_t pid = waitpid(childprocess.back(), &status, WUNTRACED);
    childprocess.pop_back();
    counter += 1;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != EXIT_SUCCESS) {
      std::cerr << "FATAL: Process (pid: " << pid << ") abnormal termination"
                << std::endl;
      for (auto pid : childprocess) {
        kill(pid, SIGKILL);
      }
      return -1;
    }
  }
  return 0;
}

bool createMissingFile(const std::string& parallel,
                       std::vector<std::string>& childids,
                       const std::string& dataPath) {
  std::ofstream outf;
  std::string filePrefix;
  std::string filePath;
  std::string moveFiles;
  for (const std::string& tableName : tableNames) {
    filePrefix = dataPath + "/" + tableName;
    for (const std::string& childid : childids) {
      filePath = filePrefix + "_" + childid + "_" + parallel + ".dat";
      outf.open(filePath, std::ios::app);
      if (!outf.is_open()) {
        std::cerr << "ERROR create file: " << filePath << std::endl;
        return false;
      }
      outf.close();
    }
    moveFiles = "mkdir -p " + filePrefix + ";mv " + dataPath + "/" + tableName +
                "_[0-9]*.dat " + filePrefix;
    if (system(moveFiles.c_str()) != 0) {
      std::cerr << "FAILED: " << moveFiles << std::endl;
      return false;
    }
  }
  return true;
}

void initEnv(const std::string& dataPath) {
  const std::string cleanRemainProcess =
      "ps -ef | grep '\\-terminate N \\-force Y'| grep -v grep | awk '{print "
      "$2}' | "
      "xargs kill -9 >/dev/null 2>&1";
  std::cout << cleanRemainProcess << std::endl;
  system(cleanRemainProcess.c_str());
  const std::string cleanRemainData =
      "rm -rf " + dataPath + "; mkdir -p " + dataPath;
  std::cout << cleanRemainData << std::endl;
  if (dataPath != "/") system(cleanRemainData.c_str());
}

std::string getGPHOME() {
  std::string gphome = "/usr/local/hawq";
  auto ptr = getenv("GPHOME");
  if (ptr != nullptr) {
    gphome = std::string(ptr);
  } else {
    std::cout << "Warning: Not found Definition of env GPHOME ,use default: "
              << gphome << std::endl;
  }
  return gphome;
}

void printHelp() {
  std::cout << "It's a dangerous test tool !!!, Don't run it if you don't know "
               "what will happend.\n"
            << "parallel_dsdgen scale parallel datapath childid...\n"
            << "eg. parallel_dsdgen 1 8 /tmp/tpcds/data 1 2 3 4" << std::endl;
}

bool generateData(const std::string& gphome, const std::string& scale,
                  const std::string& parallel,
                  std::vector<std::string>& childids,
                  const std::string& dataPath) {
  if (parallelGenData(getGPHOME(), scale, parallel, childids, dataPath) != 0) {
    std::cerr << "Generate data FAILED !!!" << std::endl;
    return false;
  };
  return createMissingFile(parallel, childids, dataPath);
}

int main(int argc, char* argv[]) {
  const int minParam = 4;
  if (argc <= minParam) {
    std::cerr << "Error: "
              << "Too few arguments specified" << std::endl;
    printHelp();
    exit(EXIT_FAILURE);
  }
  std::string scale = argv[1];
  std::string parallel = argv[2];
  const std::string dataPath = argv[3];
  std::vector<std::string> childids;
  for (int i = minParam; i != argc; ++i) {
    childids.push_back(argv[i]);
  }
  initEnv(dataPath);
  if (!generateData(getGPHOME(), scale, parallel, childids, dataPath)) {
    exit(EXIT_FAILURE);
  };
  std::cout << "Generate data SUCCESSED" << std::endl;
  exit(EXIT_SUCCESS);
}

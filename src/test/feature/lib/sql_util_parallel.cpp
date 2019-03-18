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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <thread>
#include <fstream>
#include <stdio.h>
#include "sql_util_parallel.h"
#include "string_util.h"



using std::string;

namespace hawq {
namespace test {
ThreadMessage::ThreadMessage(uint16_t tid)
{
    command = NULLCOMMAND;
    messageinfo = "";
    isrunning = false;
    thread_id = tid;
    waittids.clear();
    waitfortids.clear();
    submit = 1 ; //commit
}

ThreadMessage::~ThreadMessage()
{
    waittids.clear();
    waitfortids.clear();
}

SQLUtilityParallel::SQLUtilityParallel() {
    outputstream = NULL;
    exclusive = false;
    srand((unsigned)time(NULL));
}

SQLUtilityParallel::~SQLUtilityParallel() {
}

int32_t SQLUtilityParallel::__getTotalSession(std::fstream &in) {
    string line;
    int sessionnum = 0;
    while (getline(in, line))
    {
        if (hawq::test::startsWith(line, "#"))
            continue;
        if (!hawq::test::startsWith(hawq::test::lower(line), "total"))
        {
            EXPECT_TRUE(false) << "The file format is error, "
                    "it should begin with total:number of sessions ";
            return 0;
        }
        auto totalstring = hawq::test::split(line, ':');
        if (totalstring.size() != 2 )
            return false;
        try
        {
            sessionnum = std::stoi(hawq::test::trim(totalstring[1]));
        }
        catch(...)
        {
            return 0;
        }
        break;
    }
    return sessionnum;
}


bool SQLUtilityParallel::__processLine(const string &line, const int32_t totalsession,
                                               int32_t &sessionid, int32_t &waittid, string &message)
{
    auto querystring = hawq::test::split(line, ':');
    if (querystring.size() < 2 )
        return false;
    sessionid = std::stoi(hawq::test::trim(querystring[0])) ;
    if (sessionid < 0 || sessionid > totalsession)
        return false;
    message = hawq::test::trim(querystring[querystring.size() - 1]);
    thread_mutex.lock();
    if(querystring.size() >= 3) {
        waittid = 1;
        if(!this->exclusive){
            for (int i = 1; i < querystring.size() - 1; i++) {
                int wait_tid = std::stoi(hawq::test::trim(querystring[i]));
                threadmessage[wait_tid]->waittids.push_back(sessionid);
                threadmessage[sessionid]->waitfortids.push_back(wait_tid);
            }
        }
    }
    thread_mutex.unlock();
    return true;
}


bool SQLUtilityParallel::runParallelControlFile_exclusive(const string &sqlFile, const string &outputFile)
{
    string line;
    bool result = true;
    int32_t totalsession = 0 , i = 0 , commitnum = 0 ,blocknum = 0,commit=0;
    std::vector<std::unique_ptr<std::thread>> querythread;

    // Step 1: Open input and output file
    std::fstream in;
    in.open(sqlFile, std::ios::in);
    outputstream = fopen(outputFile.c_str(), "w");
    if (!in.is_open() || !outputstream) {
        EXPECT_TRUE(false) << "Error opening input/output file ";
        result = false;
        goto clear;
    }
    // Step 2: clear env and get total session number
    threadmessage.clear();
    totalsession = __getTotalSession(in);
    if (totalsession <= 0 )
    {
        result = false;
        goto clear;
    }
    try
    {
        // Step 3: Initialize thread message, query thread, and connection pool
        std::unique_ptr<ThreadMessage> thrm(new ThreadMessage(0));
        thrm->command = NEXTLINE;
        threadmessage.push_back(std::move(thrm));
        string schemamessage = "SET SEARCH_PATH = " + this->getSchemaName() + ";" ;
        string connstr = this->getConnectionString();
        //begin
        for(i = 1 ; i <= totalsession; i++ ){
            std::unique_ptr<ThreadMessage> thrm(new ThreadMessage(i));
            threadmessage.push_back(std::move(thrm));
        }
        for(i = 1 ; i <= totalsession; i++ ) {
            std::unique_ptr<std::thread> thr(new std::thread
                                                     (&SQLUtilityParallel::thread_execute_exclusive, this,  i, schemamessage, connstr , totalsession));
            querythread.push_back(std::move(thr));
        }
        // Step 4: Read every line of file and pass to sub thread
        while (getline(in, line))
        {
            if (hawq::test::startsWith(line, "#"))
                continue;
            if (hawq::test::startsWith(line, "$"))
            {
            		commit=1;
            		continue;
            }
            int32_t sessionid = 0 , waittid = 0;
            string message = "";
            result = __processLine(line, totalsession, sessionid, waittid, message);
            if (!result)
                goto clear;
            if (sessionid == 0)
            {
                this->execute(message, true);
            }
            else
            {
                if ( threadmessage[sessionid]->command != NULLCOMMAND )
                {
                    EXPECT_TRUE(false) << "The file format is error or query handling error\n";
                    result = false;
                    goto clear;
                }
                MessageCommandType cmd = (waittid == 0) ? SQLSTRING : BLOCKEDSQL;
                if(cmd == SQLSTRING){
                    blocknum++;
                }
                //lock
                thread_mutex.lock();
                threadmessage[sessionid]->command = cmd;
                threadmessage[sessionid]->messageinfo = message;
                threadmessage[0]->command = NULLCOMMAND;
                if(threadmessage[sessionid]->messageinfo.find("vacuum") != string::npos)
                {
                    threadmessage[sessionid]->submit = -2; //vacuum
                }
                else if(threadmessage[sessionid]->messageinfo.find("drop table") != string::npos){
                    threadmessage[sessionid]->submit = -1; //rollback
                }
                else{
                    int32_t n = random() % 2;
                    if(n == 0)
                        threadmessage[sessionid]->submit = 1; //commit
                    else
                        threadmessage[sessionid]->submit = -1; //rollback
                }
                thread_mutex.unlock();
            }

            while (threadmessage[0]->command == NULLCOMMAND  ) {}
            if (threadmessage[0]->command == BADSTATUS)
                break;
        }

        // Step 5: Set commit/rollback command
        int32_t j = 0;
        while(commitnum < totalsession || blocknum < totalsession){
            j = (j+1) % totalsession ;
            if( j == 0 )
            {
                j = totalsession;
            }
            if(threadmessage[j]->command == BLOCKOVER){
                thread_mutex.lock();
                threadmessage[j]->command = PRINTRESULT;
                thread_mutex.unlock();
                while(threadmessage[j]->command == PRINTRESULT){}
                blocknum++;
            }
            if(threadmessage[j]->submit == -2 && threadmessage[j]->command == NULLCOMMAND)
            {
                commitnum ++;
                thread_mutex.lock();
                threadmessage[j]->submit = 0;
                thread_mutex.unlock();
            }
            else if(threadmessage[j]->submit != 0 && threadmessage[j]->command == NULLCOMMAND){
                commitnum ++;
                thread_mutex.lock();
                threadmessage[j]->command = SQLSTRING;
                if(commit&&j%2==1)
                    threadmessage[j]->messageinfo = "commit;";
                else
                    threadmessage[j]->messageinfo = "rollback;";
                threadmessage[0]->command = NULLCOMMAND;
                thread_mutex.unlock();
                while (threadmessage[0]->command == NULLCOMMAND  ) {}
                thread_mutex.lock();
                threadmessage[j]->submit = 0;
                thread_mutex.unlock();
            }
        }

        // Step 6: Setup exit signal to every sub-thread
        thread_mutex.lock();
        for(i = 1 ; i <= totalsession; i++ )
        {
            threadmessage[i]->command = EXITTHREAD;
            threadmessage[i]->messageinfo = "";
        }
        thread_mutex.unlock();
        for(i = 0 ; i < totalsession; i++ ){
            querythread[i]->join();
        }
        // Step 7: Check status of every thread
        for(i = 0 ; i <= totalsession; i++ )
        {
            if (threadmessage[i]->isrunning ||
                threadmessage[i]->messageinfo.size() > 0 ||
                threadmessage[i]->command == BADSTATUS)
            {
                EXPECT_TRUE(false) << "There is not finished or error job";
                result = false;
                goto clear;
            }
        }
    }

    catch(...)
    {
        EXPECT_TRUE(false) << "There is exception \n";
        result = false;
        goto clear;
    }
    clear:
        threadmessage.clear();
    in.close();
    fclose(outputstream);
    outputstream = NULL;
    return result;
}

void SQLUtilityParallel::thread_execute_exclusive(uint16_t tid, const std::string &schemapath, const std::string &connstr , int32_t totalsession)
{
    std::unique_ptr<LibPQSQL> libpqconn;
    libpqconn.reset(new LibPQSQL(connstr));
    if (!libpqconn->connectDb())
        goto errorclear;
    if (!libpqconn->execQuery(schemapath,outputstream))
        goto errorclear;
    do
    {
        thread_mutex.lock();
        MessageCommandType cmd = threadmessage[tid]->command;
        string message = threadmessage[tid]->messageinfo;
        thread_mutex.unlock();

        switch (cmd)
        {
            case EXITTHREAD:
            {
                libpqconn->disconnectDb();
                libpqconn.release();
                return;
            }
            case BLOCKEDSQL:
            {
                thread_mutex.lock();
                std::cout<<"Tid: "<<tid<<" Blocked "<<threadmessage[tid]->messageinfo<<std::endl;
                threadmessage[tid]->isrunning = true;
                fprintf(outputstream, "%d:blocked:%s\n", tid, message.c_str());
                threadmessage[0]->command = NEXTLINE;
                thread_mutex.unlock();

                //wait for result from database
                bool result = libpqconn->execQuery(message,outputstream);
                if (!result) {
                    goto errorclear;
                }

                thread_mutex.lock();
                threadmessage[tid]->isrunning = false;
                threadmessage[tid]->command = BLOCKOVER;
                std::cout<<"Tid: "<<tid<<" Block over "<<std::endl;
                thread_mutex.unlock();
                break;
            }
            case BLOCKOVER :
                break;
            case PRINTRESULT:
            {
                thread_mutex.lock();
                std::cout<<"Tid: "<<tid<<" Print "<<std::endl;
                fprintf(outputstream, "%d:back:%s\n", tid, message.c_str());
                libpqconn->printQueryResult(outputstream);
                threadmessage[tid]->command = NULLCOMMAND;
                threadmessage[tid]->messageinfo = "";
                thread_mutex.unlock();
                break;
            }
            case SQLSTRING:
            {
                thread_mutex.lock();
                std::cout<<"Tid: "<<tid<<" SQLstring "<<threadmessage[tid]->messageinfo<<std::endl;
                if (threadmessage[tid]->isrunning)
                    goto errorclearlock;
                fprintf(outputstream, "%d:%s\n", tid, message.c_str());
                //submit sql
                if (message.compare("commit;") == 0 ||
                    message.compare("rollback;") == 0 ||
                    threadmessage[tid]->messageinfo.find("vacuum") != string::npos ||
                    threadmessage[tid]->messageinfo.find("VACUUM") != string::npos)
                {
                    for (uint16_t i = 0; i < threadmessage[tid]->waittids.size(); i++)
                    {
                        if (!threadmessage[threadmessage[tid]->waittids[i]]->isrunning)
                        {
                            goto errorclearlock;
                        }
                    }
                    bool result = libpqconn->execQuery(message,outputstream);
                    if (!result)
                        goto errorclearlock;
                }
                else
                {
                    bool result = libpqconn->execQuery(message,outputstream);
                    if (!result)
                        goto errorclearlock;
                }
                libpqconn->printQueryResult(outputstream);
                threadmessage[tid]->command = NULLCOMMAND;
                threadmessage[tid]->messageinfo = "";
                threadmessage[0]->command = NEXTLINE;
                thread_mutex.unlock();
                break;
            }
            case NULLCOMMAND:
                break;
            case NEXTLINE:
            case BADSTATUS:
            {
                goto errorclear;
            }
        }
    } while (true);

    errorclearlock:
    thread_mutex.unlock();
    errorclear:
    thread_mutex.lock();
    threadmessage[0]->command = BADSTATUS;
    threadmessage[tid]->command = BADSTATUS;
    thread_mutex.unlock();
    libpqconn->disconnectDb();
    libpqconn.release();
}

bool SQLUtilityParallel::runParallelControlFile(const string &sqlFile, const string &outputFile)
{
    string line;
    bool result = true;
    int32_t totalsession = 0, i = 0;
    std::vector<std::unique_ptr<std::thread>> querythread;
    // Step 1: Open input and output file
    std::fstream in;
    in.open(sqlFile, std::ios::in);
    outputstream = fopen(outputFile.c_str(), "w");
    if (!in.is_open() || !outputstream) {
        EXPECT_TRUE(false) << "Error opening input/output file ";
        result = false;
        goto clear;
    }
    // Step 2: clear env and get total session number
    threadmessage.clear();
    totalsession = __getTotalSession(in);
    if (totalsession <= 0 )
    {
        result = false;
        goto clear;
    }
    try
    {
        // Step 3: Initialize thread message, query thread, and connection pool
        std::unique_ptr<ThreadMessage> thrm(new ThreadMessage(0));
        thrm->command = NEXTLINE;
        threadmessage.push_back(std::move(thrm));
        string schemamessage = "SET SEARCH_PATH = " + this->getSchemaName() + ";" ;
        string connstr = this->getConnectionString();
        //begin
        for(i = 1 ; i <= totalsession; i++ ){
            std::unique_ptr<ThreadMessage> thrm(new ThreadMessage(i));
            threadmessage.push_back(std::move(thrm));
        }
        for(i = 1 ; i <= totalsession; i++ ) {
            std::unique_ptr<std::thread> thr(new std::thread
                                                     (&SQLUtilityParallel::thread_execute, this,  i, schemamessage, connstr));
            querythread.push_back(std::move(thr));
        }

        // Step 4: Read every line of file and pass to sub thread
        while (getline(in, line))
        {
            if (hawq::test::startsWith(line, "#"))
                continue;
            int32_t sessionid = 0 , waittid = 0;
            string message = "";

            result = __processLine(line, totalsession, sessionid, waittid, message);
            if (!result)
                goto clear;

            if (sessionid == 0)
            {
                this->execute(message, true);
            }
            else
            {
                if ( threadmessage[sessionid]->command != NULLCOMMAND )
                {
                    EXPECT_TRUE(false) << "The file format is error or query handling error\n";
                    result = false;
                    goto clear;
                }
                MessageCommandType cmd = (waittid == 0) ? SQLSTRING : BLOCKEDSQL;
                //lock
                thread_mutex.lock();
                threadmessage[sessionid]->command = cmd;
                threadmessage[sessionid]->messageinfo = message;
                threadmessage[0]->command = NULLCOMMAND;
                thread_mutex.unlock();
            }
            while (threadmessage[0]->command == NULLCOMMAND  ) {}
            if (threadmessage[0]->command == BADSTATUS)
                break;
        }

        // Step 5: Setup exit signal to every sub-thread
        thread_mutex.lock();
        for(i = 1 ; i <= totalsession; i++ )
        {
            threadmessage[i]->command = EXITTHREAD;
            threadmessage[i]->messageinfo = "";
        }
        thread_mutex.unlock();
        for(i = 0 ; i < totalsession; i++ ){
            querythread[i]->join();
        }
        // Step 6: Check status of every thread
        for(i = 0 ; i <= totalsession; i++ )
        {
            if (threadmessage[i]->isrunning ||
                threadmessage[i]->messageinfo.size() > 0 ||
                threadmessage[i]->command == BADSTATUS)
            {
                EXPECT_TRUE(false) << "There is not finished or error job";
                result = false;
                goto clear;
            }
        }
    }

    catch(...)
    {
        EXPECT_TRUE(false) << "There is exception \n";
        result = false;
        goto clear;
    }
    clear:
    threadmessage.clear();
    in.close();
    fclose(outputstream);
    outputstream = NULL;
    return result;
}

void SQLUtilityParallel::thread_execute(uint16_t tid, const std::string &schemapath, const std::string &connstr)
{
    std::unique_ptr<LibPQSQL> libpqconn;
    libpqconn.reset(new LibPQSQL(connstr));
    if (!libpqconn->connectDb())
        goto errorclear;
    if (!libpqconn->execQuery(schemapath,outputstream))
        goto errorclear;
    do
    {
        thread_mutex.lock();
        MessageCommandType cmd = threadmessage[tid]->command;
        string message = threadmessage[tid]->messageinfo;
        thread_mutex.unlock();

        switch (cmd)
        {
            case EXITTHREAD:
            {
                libpqconn->disconnectDb();
                libpqconn.release();
                return;
            }
            case BLOCKEDSQL:
            {
                thread_mutex.lock();
                threadmessage[tid]->isrunning = true;
                std::cout<<"BLOCK Tid: "<<tid<<" "<<threadmessage[tid]->messageinfo<<std::endl;
                fprintf(outputstream, "%d:blocked:%s\n", tid, message.c_str());
                threadmessage[0]->command = NEXTLINE;
                thread_mutex.unlock();
                bool result = libpqconn->execQuery(message,outputstream);
                if (!result) {
                    std::cout<<"Error"<<std::endl;
                    goto errorclear;
                }

                thread_mutex.lock();
                threadmessage[tid]->isrunning = false;
                threadmessage[tid]->command = BLOCKOVER;
                std::cout<<"BLOCKOVER Tid: "<<tid<<" "<<threadmessage[tid]->messageinfo<<std::endl;
                thread_mutex.unlock();
                break;
            }
            case BLOCKOVER :
                break;
            case PRINTRESULT:
            {
                thread_mutex.lock();
                std::cout<<"PRINT Tid: "<<tid<<" "<<threadmessage[tid]->messageinfo<<std::endl;
                fprintf(outputstream, "%d:back:%s\n", tid, message.c_str());
                libpqconn->printQueryResult(outputstream);
                threadmessage[tid]->command = NULLCOMMAND;
                threadmessage[tid]->messageinfo = "";
                thread_mutex.unlock();
                break;
            }
            case SQLSTRING:
            {
                thread_mutex.lock();
                std::cout<<"SQL Tid: "<<tid<<" "<<threadmessage[tid]->messageinfo<<std::endl;
                if (threadmessage[tid]->isrunning)
                    goto errorclearlock;
                fprintf(outputstream, "%d:%s\n", tid, message.c_str());

                if (message.compare("commit;") == 0 ||
                    message.compare("rollback;") == 0 || threadmessage[tid]->messageinfo.find("vacuum") != string::npos) {
                    for (uint16_t i = 0; i < threadmessage[tid]->waittids.size(); i++) {
                        if (!threadmessage[threadmessage[tid]->waittids[i]]->isrunning) {
                            goto errorclearlock;
                        }
                    }

                    bool result = libpqconn->execQuery(message,outputstream);
                    if (!result)
                        goto errorclearlock;
                    thread_mutex.unlock();

                    while (threadmessage[tid]->waittids.size() > 0) {
                        thread_mutex.lock();
                        int waitpid = threadmessage[tid]->waittids[0];

                        for (std::vector<int>::iterator iter = threadmessage[waitpid]->waitfortids.begin();
                             iter != threadmessage[waitpid]->waitfortids.end(); iter++) {
                            if (*iter == tid) {
                                threadmessage[waitpid]->waitfortids.erase(iter);
                                break;
                            }
                        }

                        thread_mutex.unlock();

                        if (threadmessage[waitpid]->waitfortids.size() == 0) {
                            thread_mutex.unlock();
                            while (threadmessage[waitpid]->command != BLOCKOVER) {}
                            thread_mutex.lock();
                            threadmessage[waitpid]->command = PRINTRESULT;
                            thread_mutex.unlock();
                        }

                        while (threadmessage[waitpid]->command == PRINTRESULT) {}
                        thread_mutex.lock();
                        threadmessage[tid]->waittids.erase(threadmessage[tid]->waittids.begin());
                        thread_mutex.unlock();
                    }

                    thread_mutex.lock();
                }
                else
                {
                    bool result = libpqconn->execQuery(message,outputstream);
                    if (!result)
                        goto errorclearlock;
                }
                libpqconn->printQueryResult(outputstream);
                threadmessage[tid]->command = NULLCOMMAND;
                threadmessage[tid]->messageinfo = "";
                threadmessage[0]->command = NEXTLINE;
                thread_mutex.unlock();
                break;
            }
            case NULLCOMMAND:
                break;
            case NEXTLINE:
            case BADSTATUS:
            {
                goto errorclear;
            }
        }
    } while (true);

    errorclearlock:
    thread_mutex.unlock();
    errorclear:
    thread_mutex.lock();
    threadmessage[0]->command = BADSTATUS;
    threadmessage[tid]->command = BADSTATUS;
    thread_mutex.unlock();
    libpqconn->disconnectDb();
    libpqconn.release();
}


void SQLUtilityParallel::execParallelControlSQLFile(const string &sqlFile,
                                                            const string &ansFile,
                                                            bool exclusive,
                                                            const string &initFile)
{
    FilePath fp;
    const string ansFileAbsPath = testRootPath + "/" + ansFile;
    const string sqlFileAbsPath = testRootPath + "/" + sqlFile;
    if (!std::ifstream(ansFileAbsPath))
        ASSERT_TRUE(false) << ansFileAbsPath << " doesn't exist";
    if (!std::ifstream(sqlFileAbsPath))
        ASSERT_TRUE(false) << sqlFileAbsPath << " doesn't exist";
    //get file from source

    fp = splitFilePath(ansFileAbsPath);
    // double check to avoid empty fileBaseName
    if (fp.fileBaseName.empty())
        ASSERT_TRUE(false) << ansFileAbsPath << " is invalid";

    // outFile is located in the same folder with ansFile
    const string outFileAbsPath = fp.path + "/" + fp.fileBaseName + ".out";

    bool result;
    this->exclusive = exclusive ;
    if( this->exclusive )
        result = this->runParallelControlFile_exclusive(sqlFileAbsPath, outFileAbsPath);
    else
        result = this->runParallelControlFile(sqlFileAbsPath, outFileAbsPath);

    if (!result)
        ASSERT_TRUE(false) << " Execute query is failed";

    //Check result
    if( !this->exclusive )
        checkAnsFile(ansFileAbsPath, outFileAbsPath, initFile, "", fp);
}
    } // namespace test
} // namespace hawq

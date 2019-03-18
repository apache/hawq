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

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include "psql.h"

//
#include <iostream>
#include "command.h"
#include "../../../interfaces/libpq/libpq-int.h"

using std::string;

namespace hawq {
namespace test {

#define PSQL_BASIC_DIFF_OPTS "-w -I NOTICE: -I HINT: -I CONTEXT: -I GP_IGNORE:"
#define PSQL_PRETTY_DIFF_OPTS \
  "-w -I NOTICE: -I HINT: -I CONTEXT: -I GP_IGNORE: -C3"

void PSQLQueryResult::setErrorMessage(const string & errmsg) {
  this->_errmsg = errmsg;
}

const string& PSQLQueryResult::getErrorMessage() const {
  return this->_errmsg;
}

bool PSQLQueryResult::isError() const {
  return this->_errmsg.length() > 0;
}

const std::vector<std::vector<string> >& PSQLQueryResult::getRows() const {
  return this->_rows;
}

const std::vector<string>& PSQLQueryResult::getFields() const {
  return this->_fields;
}

const std::vector<int>& PSQLQueryResult::getFieldSizes() const {
  return this->_fieldsizes;
}


const std::vector<string>& PSQLQueryResult::getRow(int ri) const {
  return this->getRows()[ri];
}

const string& PSQLQueryResult::getData(int ri, int ci) const {
  return this->getRow(ri)[ci];
}

string PSQLQueryResult::getData(int ri, const string& ck) const {
  for (unsigned int ci = 0; ci < this->_fields.size(); ci++) {
    if (ck == this->_fields[ci]) {
      return this->getData(ri, ci);
    }
  }
  return "";
}

const string& PSQLQueryResult::getFieldName(int ci) const {
  return this->_fields[ci];
}

int PSQLQueryResult::rowCount() const {
  return this->_rows.size();
}

int PSQLQueryResult::fieldCount() const {
  return this->_fields.size();
}

void PSQLQueryResult::savePGResult(const PGresult* res) {
  int nfields = PQnfields(res);
  for (int i = 0; i < nfields; i++) {
    this->_fields.push_back(PQfname(res, i));
    this->_fieldsizes.push_back(PQfsize(res,i));
  }
  for (int i = 0; i < PQntuples(res); i++) {
    std::vector<string> row;
    for (int j = 0; j < nfields; j++) {
      row.push_back(PQgetvalue(res, i, j));
      int size = row[j].size();
      int size2 = _fieldsizes[j];
      if (size > size2)
      {
         _fieldsizes[j] = size;
      }
    }
    this->_rows.push_back(row);
  }
}

void PSQLQueryResult::reset() {
  this->_errmsg.clear();
  this->_rows.clear();
  this->_fields.clear();
  this->_fieldsizes.clear();
}

PSQL& PSQL::runSQLCommand(const string& sql_cmd) {
  hawq::test::Command c(this->_getPSQLQueryCommand(sql_cmd));
  c.run();
  this->_last_status = c.getResultStatus();
  this->_last_result = c.getResultOutput();
  return *this;
}

PSQL& PSQL::runSQLFile(const string& sql_file, bool printTupleOnly) {
  this->_last_status =
      hawq::test::Command::getCommandStatus(this->_getPSQLFileCommand(sql_file, printTupleOnly));
  return *this;
}

PSQL& PSQL::runSQLFile(const string& sql_file, const string &psqlOptions, bool printTupleOnly) {
  this->_last_status =
      hawq::test::Command::getCommandStatus(this->_getPSQLFileCommand(sql_file, psqlOptions, printTupleOnly));
  return *this;
}

bool PSQL::testConnection() {
  PGconn* conn = NULL;
  conn = PQconnectdb(this->getConnectionString().c_str());
  if (PQstatus(conn) != CONNECTION_OK) {
     this->_result.setErrorMessage(PQerrorMessage(conn));
     return false;
  }
  return true;
}
const PSQLQueryResult& PSQL::getQueryResult(const string& sql) {
  PGconn* conn = NULL;
  PGresult* res = NULL;

  conn = PQconnectdb(this->getConnectionString().c_str());
  if (PQstatus(conn) != CONNECTION_OK) {
    this->_result.setErrorMessage(PQerrorMessage(conn));
    goto done;
  }

  res = PQexec(conn, sql.c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    this->_result.setErrorMessage(PQerrorMessage(conn));
    goto done;
  }
  this->_result.reset();
  this->_result.savePGResult(res);

done:
  if (res) {
    PQclear(res);
    res = NULL;
  }
  if (conn) {
    PQfinish(conn);
    conn = NULL;
  }
  return this->_result;
}

string PSQL::getHost(){
	return this->_host;
}
PSQL& PSQL::setHost(const string& host) {
  this->_host = host;
  return *this;
}

PSQL& PSQL::setDatabase(const std::string& db){
  this->_dbname = db;
  return *this;
}

PSQL& PSQL::setPort(const string& port) {
  this->_port = port;
  return *this;
}

PSQL& PSQL::setUser(const string& username) {
  this->_user = username;
  return *this;
}

PSQL& PSQL::setPassword(const string& password) {
  this->_password = password;
  return *this;
}

PSQL& PSQL::setOutputFile(const string& out) {
  this->_output_file = out;
  return *this;
}

void PSQL::resetOutput() {
  this->_output_file.clear();
}

string PSQL::getConnectionString() const {
  // host=localhost port=5432 dbname=mydb
  string command;
  command.append("host=")
      .append(this->_host)
      .append(" port=")
      .append(this->_port)
      .append(" user=")
      .append(this->_user)
      .append(" dbname=")
      .append(this->_dbname);
  return command;
}

int PSQL::getLastStatus() const {
  return this->_last_status;
}

const string& PSQL::getLastResult() const {
  return this->_last_result;
}

const string PSQL::_getPSQLBaseCommand() const {
  string command = "psql";
  command.append(" -p ").append(this->_port);
  command.append(" -h ").append(this->_host);
  command.append(" -U ").append(this->_user);
  command.append(" -d ").append(this->_dbname);
  if (this->_output_file.length() > 0) {
    command.append(" > ").append(this->_output_file);
  }
  return command;
}

const string PSQL::_getPSQLQueryCommand(const string& query) const {
  string command = this->_getPSQLBaseCommand();
  return command.append(" -c \"").append(query).append("\"");
}

const string PSQL::_getPSQLFileCommand(const string& file, bool printTupleOnly) const {
  string command = this->_getPSQLBaseCommand();
  if (printTupleOnly) {
	  return command.append(" -a -A -t -f").append(file);
  }
  else {
	  return command.append(" -a -f ").append(file);
  }
}

const string PSQL::_getPSQLFileCommand(const string& file, const string& psqlOptions, bool printTupleOnly) const {
  string command = this->_getPSQLBaseCommand();
  command.append(psqlOptions);
  if (printTupleOnly) {
	  return command.append(" -a -A -t -f").append(file);
  }
  else {
	  return command.append(" -a -f ").append(file);
  }
}

bool PSQL::checkDiff(const string& expect_file,
                     const string& result_file,
                     bool save_diff,
                     const string& global_init_file,
                     const string& init_file) {
  string diff_file = getDifFilePath(result_file);
  string command;
  command.append("gpdiff.pl -du ")
      .append(PSQL_BASIC_DIFF_OPTS);
  if (!global_init_file.empty()) {
      command.append(" -gpd_init ")
          .append(global_init_file);
  }
  if (!init_file.empty()) {
      command.append(" -gpd_init ")
          .append(init_file);
  }
  command.append(" ")
      .append(expect_file)
      .append(" ")
      .append(result_file)
      .append(" ")
      .append(" >")
      .append(diff_file);

  if (hawq::test::Command::getCommandStatus(command) == 0) {
    unlink(diff_file.c_str());
    return false;
  } else {
    if (!save_diff) {
      unlink(diff_file.c_str());
    }
    return true;
  }
}

std::string PSQL::getDifFilePath(const std::string&result_file){
  return result_file + ".diff";
}



LibPQSQL::LibPQSQL(const std::string &connstring)
{
	libpqconn = NULL;
	connstr = connstring;
}

LibPQSQL::~LibPQSQL()
{
	disconnectDb();
}

bool LibPQSQL::connectDb()
{
    libpqconn = PQconnectdb(this->connstr.c_str());
    if (PQstatus(libpqconn) != CONNECTION_OK) {
        this->_result.setErrorMessage(PQerrorMessage(libpqconn));
    	if (libpqconn) {
    	    PQfinish(libpqconn);
    	    libpqconn = NULL;
    	}
        return false;
    }
    return true;
}


bool LibPQSQL::execQuery(const string& sql,FILE* outputfile)
{
    if (!libpqconn)
        return false;

    PGresult* res = NULL;
    bool status = true;
    res = PQexec(libpqconn, sql.c_str());

    if(PQresultStatus(res)!=PGRES_TUPLES_OK&&strcmp(res->cmdStatus,"SET")!=0)
    	if(outputfile!=NULL)
    		fprintf(outputfile,"%s\n",res->cmdStatus);
    switch (PQresultStatus(res))
    {
        case PGRES_TUPLES_OK:
        {
            this->_result.reset();
            this->_result.savePGResult(res);
            break;
        }
        case PGRES_COMMAND_OK:
        {
            this->_result.reset();
            this->_result.savePGResult(res);
            break;
        }
        case PGRES_BAD_RESPONSE:
        case PGRES_NONFATAL_ERROR:
        case PGRES_FATAL_ERROR:
        {
            this->_result.reset();
            //this->_result.savePGResult(res);
            this->_result.setErrorMessage(res->errMsg);
            break;
        }
        default:
        {
            this->_result.setErrorMessage(PQerrorMessage(libpqconn));
            status = false;
            break;
        }
    }
    if (res)
    {
        PQclear(res);
        res = NULL;
    }
    return status;
}

int32_t LibPQSQL::getRownum()
{
    const std::vector<std::vector<std::string>> rowdata = this->_result.getRows();
    return rowdata.size();
}


void LibPQSQL::printQueryResult(FILE* outputfile)
{
    if(this->_result.isError()){
        fprintf(outputfile,"%s",this->_result.getErrorMessage().c_str());
        return ;
    }
    const std::vector<std::string> fields = this->_result.getFields();
    if (fields.size() == 0 )
        return;

    const std::vector<std::vector<std::string>> rowdata = this->_result.getRows();
    const std::vector<int> fieldsize = this->_result.getFieldSizes();

    int totalsize = 0;
    uint16_t i = 0;
    string formatstring;
    for ( i = 0; i < fields.size() ; i++)
    {
        totalsize += fieldsize[i] + 3;
        if (i == 0)
            formatstring = " %-" + std::to_string(fieldsize[i]) + "s ";
        else
            formatstring = "| %-" + std::to_string(fieldsize[i]) + "s ";
        fprintf(outputfile, formatstring.c_str(), fields[i].c_str());
    }
    fprintf(outputfile, "\n");
    for ( i = 0; i < totalsize ; i++)
    {
        fprintf(outputfile, "%s", "-");
    }
    fprintf(outputfile, "\n");

    for ( i = 0; i < rowdata.size() ; i++)
    {
        for ( uint16_t j = 0; j < rowdata[i].size() ; j++)
        {
            if (j == 0)
                formatstring = " %-" + std::to_string(fieldsize[j]) + "s ";
            else
                formatstring = "| %-" + std::to_string(fieldsize[j]) + "s ";
            fprintf(outputfile, formatstring.c_str(), rowdata[i][j].c_str());
        }
        fprintf(outputfile, "\n");
    }
    if (rowdata.size() == 0)
        fprintf(outputfile, "(0 row)\n");
    else
        fprintf(outputfile, "(%lu rows)\n", rowdata.size());
}
bool LibPQSQL::disconnectDb()
{
    if (libpqconn) {
        PQfinish(libpqconn);
        libpqconn = NULL;
        return true;
    }
    return false;
}

} // namespace test
} // namespace hawq

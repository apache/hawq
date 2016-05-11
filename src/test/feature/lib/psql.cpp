#include <iostream>
#include <unistd.h>
#include "psql.h"
#include "command.h"

#define PSQL_BASIC_DIFF_OPTS    "-w -I NOTICE: -I HINT: -I CONTEXT: -I GP_IGNORE:"
#define PSQL_PRETTY_DIFF_OPTS   "-w -I NOTICE: -I HINT: -I CONTEXT: -I GP_IGNORE: -C3"

void PSQLQueryResult::setErrorMessage(const std::string errmsg)
{
    this->_errmsg = errmsg;
}

const std::string& PSQLQueryResult::getErrorMessage() const
{
    return this->_errmsg;
}

bool PSQLQueryResult::isError() const
{
    return this->_errmsg.length() > 0;
}

const std::vector<std::vector<std::string> >& PSQLQueryResult::getRows() const
{
    return this->_rows;
}

const std::vector<std::string>& PSQLQueryResult::getFields() const
{
    return this->_fields;
}

const std::vector<std::string>& PSQLQueryResult::getRow(int ri) const
{
    return this->getRows()[ri];
}

const std::string& PSQLQueryResult::getData(int ri, int ci) const
{
    return this->getRow(ri)[ci];
}

std::string PSQLQueryResult::getData(int ri, const std::string& ck) const
{
    for (int ci=0;ci<this->_fields.size();ci++)
    {
        if (ck == this->_fields[ci])
        {
            return this->getData(ri, ci);
        }
    }
    return "";
}

const std::string& PSQLQueryResult::getFieldName(int ci) const
{
    return this->_fields[ci];
}

int PSQLQueryResult::rowCount() const
{
    return this->_rows.size();
}

int PSQLQueryResult::fieldCount() const
{
    return this->_fields.size();
}

void PSQLQueryResult::savePGResult(const PGresult *res)
{
    int i, j;
    int nfields = PQnfields(res);
    for (i=0; i<nfields; i++)
    {
        this->_fields.push_back(PQfname(res, i));
    }
    
    for (i=0; i<PQntuples(res); i++)
    {
        std::vector<std::string> row;
        for (j=0; j<nfields; j++)
        {
            row.push_back(PQgetvalue(res, i, j));
        }
        this->_rows.push_back(row);
    } 
}

void PSQLQueryResult::reset()
{
    this->_errmsg.clear();
    this->_rows.clear();
    this->_fields.clear();
}

PSQL& PSQL::runSQLCommand(const std::string& sql_cmd)
{
    Command c(this->_getPSQLQueryCommand(sql_cmd));
    c.run();
    this->_last_status = c.getResultStatus();
    this->_last_result = c.getResultOutput();
    return *this;
}

PSQL& PSQL::runSQLFile(const std::string& sql_file)
{
    this->_last_status = Command::getCommandStatus(this->_getPSQLFileCommand(sql_file)); 
    return *this;
}

const PSQLQueryResult& PSQL::getQueryResult(const std::string& sql)
{
    PGconn      *conn = NULL;
    PGresult    *res = NULL;

    conn = PQconnectdb(this->getConnectionString().c_str());
    if (PQstatus(conn) != CONNECTION_OK)
    {
        this->_result.setErrorMessage(PQerrorMessage(conn));
        goto done;
    }

    res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        this->_result.setErrorMessage(PQerrorMessage(conn));
        goto done;
    }

    this->_result.reset();
    this->_result.savePGResult(res);

done:
    if (res) 
    {
        PQclear(res);
        res = NULL;
    }
    
    if (conn)
    {
        PQfinish(conn);
        conn = NULL;
    }
    
    return this->_result;
}

PSQL& PSQL::setHost(const std::string& host)
{
    this->_host = host;
    return *this;
}

PSQL& PSQL::setPort(const std::string& port)
{
    this->_port = port;
    return *this;
}

PSQL& PSQL::setUser(const std::string& username)
{
    this->_user = username;
    return *this;
}

PSQL& PSQL::setPassword(const std::string& password)
{
    this->_password = password;
    return *this;
}

PSQL& PSQL::setOutputFile(const std::string& out)
{
    this->_output_file = out;
    return *this;
}

std::string PSQL::getConnectionString() const
{
    // host=localhost port=5432 dbname=mydb 
    std::string command;
    command.append("host=").append(this->_host)
            .append(" port=").append(this->_port)
            .append(" user=").append(this->_user)
            .append(" dbname=").append(this->_dbname);
    return command; 
}

int PSQL::getLastStatus() const
{
    return this->_last_status;
}

const std::string& PSQL::getLastResult() const
{
    return this->_last_result;
}


const std::string PSQL::_getPSQLBaseCommand() const
{
    std::string command = "psql";
    command.append(" -p ").append(this->_port);
    command.append(" -h ").append(this->_host);
    command.append(" -U ").append(this->_user);
    command.append(" -d ").append(this->_dbname);
    if (this->_output_file.length() > 0)
    {
        command.append(" -o ").append(this->_output_file);
    }
 
    return command;
}

const std::string PSQL::_getPSQLQueryCommand(const std::string& query) const
{
    std::string command = this->_getPSQLBaseCommand();
    return command.append(" -c '").append(query).append("'");
}

const std::string PSQL::_getPSQLFileCommand(const std::string& file) const
{
    std::string command = this->_getPSQLBaseCommand();
    return command.append(" -f ").append(file);
}

bool PSQL::checkDiff(const std::string& expect_file, const std::string& result_file, bool save_diff)
{
    std::string diff_file = result_file + ".diff";
    std::string command;
    command.append("gpdiff.pl ").append(PSQL_BASIC_DIFF_OPTS).append(" ")
            .append(expect_file).append(" ")
            .append(result_file).append(" ")
            .append(" >").append(diff_file);

    if (Command::getCommandStatus(command) == 0) 
    {
        unlink(diff_file.c_str());
        return false;
    }
    else
    {
        if (!save_diff)
        {
            unlink(diff_file.c_str());
        }
        return true;
    }
}

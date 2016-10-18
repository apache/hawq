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

/*-------------------------------------------------------------------------
 *
 * gpcheckhdfs.c
 *
 * This file mainly provide functionalities to check status of
 * HDFS cluster when:
 *     1. doing HAWQ cluster initialization;
 *     2. monitoring HDFS cluster health manually.
 *
 * It uses libhdfs to communicate with HDFS.
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include <grp.h>
#include <pwd.h>

#include <sys/types.h>
#include <unistd.h>

#include "krb5/krb5.h"
#include "hdfs/hdfs.h"
/*
 * Check Error Code
 */
#define GPCHKHDFS_ERR 1
#define CONNECT_ERR 100
#define OPENFILE_ERR 101
#define WRITEFILE_ERR 102
#define FLUSH_ERR 103
#define DELETE_ERR 104
#define DFSDIR_ERR 105
#define GETTOKEN_ERR 106
#define KRBLOGIN_ERR 107
#define DIRECTORY_ERR 108

char * conncat(const char * dfs_name, const char * dfs_url);
void getHostAndPort(const char * dfs_url, char * host, char * port);

/*
 * test whether hdfs can be connected while kerberos is on or not
 */
int testHdfsConnect(hdfsFS * fs, const char * host, int port,
        const char * krbstatus, const char * krb_srvname,
        const char * krb_keytabfile);

/*
 * test whether the filepath which dfs_url defined in hdfs is existed or not.
 */
int testHdfsExisted(hdfsFS fs, const char * filepath, const char * dfscompleteurl);

/*
 * test whether basic file operation in hdfs is worked well or not
 */
int testHdfsOperateFile(hdfsFS fs, const char * filepath, const char * dfscompleteurl);

int main(int argc, char * argv[]) {
    /*
    *  argv will read from conf
    *  argv[1]:dfs_name
    *  argv[2]:dfs_url
    *  argv[3]:krb status
    *  argv[4]:krb service name
    *  argv[5]:krb keytab file
    */
    if (argc < 3 || argc > 6
            || ((argc == 4 || argc == 5) && 0 != strcasecmp(argv[3], "off") && 0 != strcasecmp(argv[3], "false"))) {
        fprintf(stderr,
                "ERROR: gpcheckhdfs parameter error, Please check your config file\n"
                        "\tDFS_NAME and DFS_URL are required, KERBEROS_SERVICENAME, KERBEROS_KEYFILE and "
                        "ENABLE_SECURE_FILESYSTEM are optional\n");
        return GPCHKHDFS_ERR;
    } 

    char * dfs_name = argv[1];
    char * dfs_url = argv[2];
    char * krbstatus = NULL;
    char * krb_srvname = NULL;
    char * krb_keytabfile = NULL;

    if (argc >= 4) {
        krbstatus = argv[3];
    }

    if (argc >= 6) {
        krb_srvname = argv[4];
        krb_keytabfile = argv[5];
    }

    char * host = (char *)malloc(255 * sizeof(char));
    char * port = (char *)malloc(5 * sizeof(char));
    getHostAndPort(dfs_url, host, port);
    int iPort = atoi(port);

    if (iPort < 0) {
        fprintf(stderr, "ERROR: Invalid NameNode Port, Please Check\n");
        return CONNECT_ERR;
    }

    hdfsFS fs;
    int connErrCode = testHdfsConnect(&fs, host, iPort, krbstatus, krb_srvname,
            krb_keytabfile);

    if (connErrCode) {
        return connErrCode;
    }

    char * filename = "/testFile";
    char * dfscompleteurl = conncat(dfs_name, dfs_url);
    char * filepath = strchr(dfs_url, '/');

    if (!filepath) {
        fprintf(stderr,"ERROR: argv[2] does not contain '/'");
        return DIRECTORY_ERR;
    }

    /*
    * check hdfs's directory configured in dfs_url
    * such as sdw2:8020/gpsql/,the filepath is /gpsql
    * */
    int checkdirErrCode = testHdfsExisted(fs, filepath, dfscompleteurl);

    if (checkdirErrCode) {
        return checkdirErrCode;
    }

    strcat(filepath, filename);
    int operateErrCode = testHdfsOperateFile(fs, filepath, dfscompleteurl);

    if (operateErrCode) {
        return operateErrCode;
    }

    free(host);
    free(port);
    return 0;
}

int testHdfsOperateFile(hdfsFS fs, const char * filepath, const char * dfscompleteurl) {
    hdfsFile testFile = hdfsOpenFile(fs, filepath, O_CREAT, 0, 0, 0);

    if (NULL == testFile) {
        fprintf(stderr, "ERROR:'hdfsOpenFile' failed to create file %s\n", dfscompleteurl);
        return OPENFILE_ERR;
    }

    char * testbuff = "Test file....";
    int ts = hdfsWrite(fs, testFile, testbuff, strlen(testbuff));

    if (ts < 0) {
        fprintf(stderr, "ERROR:'hdfsWrite' failed to write to file %s\n", dfscompleteurl);
        return WRITEFILE_ERR;
    }

    int rv = hdfsHFlush(fs, testFile);

    if (rv < 0) {
        fprintf(stderr, "ERROR:'hdfsHFlush' failed to flush file\n");
        return FLUSH_ERR;
    }

    rv = hdfsCloseFile(fs, testFile);
    if (rv < 0) {
        fprintf(stderr, "ERROR:'hdfsClose' failed to close file\n");
        return FLUSH_ERR;
    }

    rv = hdfsDelete(fs, filepath, 0);

    if (rv < 0) {
        fprintf(stderr, "ERROR:'hdfsDelete' failed to delete %s\n", dfscompleteurl);
        return DELETE_ERR;
    }

    return 0;
}

int testHdfsConnect(hdfsFS * fsptr, const char * host, int iPort,
        const char * krbstatus, const char * krb_srvname,
        const char * krb_keytabfile) {
    struct hdfsBuilder * builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, host);

    if (iPort > 0)
        hdfsBuilderSetNameNodePort(builder, iPort);

    if (NULL != krbstatus && NULL != krb_keytabfile &&
            (!strcasecmp(krbstatus, "on") || !strcasecmp(krbstatus, "true"))) {   //Kerberos if On
        char * krb5_ccname = "/tmp/postgres.ccname";
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "kinit -k -t %s -c %s %s",
                 krb_keytabfile, krb5_ccname, krb_srvname);

        if (system(cmd)) {
            fprintf(stderr, "ERROR: Failed to login to Kerberos, command line: '%s'\n", cmd);
            return KRBLOGIN_ERR;
        }

        hdfsBuilderSetKerbTicketCachePath(builder, krb5_ccname);
        *fsptr = hdfsBuilderConnect(builder);

        if (NULL == (*fsptr)) {
            if (iPort > 0)
                fprintf(stderr, "ERROR: (None HA) Can not connect to 'hdfs://%s:%d'\n", host, iPort);
            else
                fprintf(stderr, "ERROR: (HA) Can not connect to 'hdfs://%s'\n", host);

            fprintf(stderr, "Please check your HDFS or hdfs-client.xml in ${GPHOME}/etc\n");
            return CONNECT_ERR;
        }

        hdfsFreeBuilder(builder);
        char * token = hdfsGetDelegationToken((*fsptr), krb_srvname);

        if (NULL == token) {
            fprintf(stderr, "ERROR: Get Delegation Token Error\n");
            return GETTOKEN_ERR;
        }

        builder = hdfsNewBuilder();
        hdfsBuilderSetNameNode(builder, host);

        if (iPort > 0)
            hdfsBuilderSetNameNodePort(builder, iPort);

        hdfsBuilderSetToken(builder, token);
    }

    *fsptr = hdfsBuilderConnect(builder);

    if (NULL == (*fsptr)) {
        fprintf(stderr, "ERROR: Can not connect to 'hdfs://%s:%d'\n", host, iPort);
        return CONNECT_ERR;
    }

    hdfsFreeBuilder(builder);
    return 0;
}

int testHdfsExisted(hdfsFS fs, const char * filepath, const char * dfscompleteurl) {
    int notExisted = hdfsExists(fs, filepath);

    if (notExisted) {
        fprintf(stderr, "WARNING:'%s' does not exist, create it ...\n", dfscompleteurl);
        int rv = hdfsCreateDirectory(fs, filepath);

        if (rv < 0) {
            fprintf(stderr, "ERROR: failed to create directory %s\n", dfscompleteurl);
            return DFSDIR_ERR;
        }
    } else {
        int num;
        hdfsFileInfo * fi = hdfsListDirectory(fs, filepath, &num);

        if (NULL == fi || num != 0) {
            fprintf(stderr, "ERROR: failed to list directory %s or it is not empty\n"
                    "Please Check your filepath before doing HAWQ cluster initialization.\n", dfscompleteurl);
            return DFSDIR_ERR;
        }
    }

    return 0;
}

char * conncat(const char * dfs_name, const char * dfs_url) {
    const char * colon = "://";
    int strlength = strlen(dfs_name) + strlen(dfs_url) + strlen(colon) + 1;
    char * dfsurl = (char *)malloc(strlength * sizeof(char));
    strcpy(dfsurl, dfs_name);
    strcat(dfsurl, colon);
    strcat(dfsurl, dfs_url);
    return dfsurl;
}

void getHostAndPort(const char * dfs_url, char * host, char * port) {
    char * colonPos = strchr(dfs_url , ':');
    char * spritPos = NULL;

    if (colonPos != NULL) { //None HA url case
        int hostlength = colonPos - dfs_url;
        strncpy(host , dfs_url , hostlength);
        *(host + hostlength) = '\0';
        char * remainPtr = colonPos + 1;
        spritPos = strchr(remainPtr , '/');
        int portlength = spritPos - remainPtr;
        strncpy(port , remainPtr , portlength);
        *(port + portlength) = '\0';
    } else {        // HA url case
        spritPos = strchr(dfs_url , '/');
        int nServicelength = spritPos - dfs_url;
        strncpy(host, dfs_url, nServicelength);
        *(host + nServicelength) = '\0';
    }
}


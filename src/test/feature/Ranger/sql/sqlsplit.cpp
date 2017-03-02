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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
    FILE *fin, *fout, *foutsuper;
    fin = fopen("sqlsplitinput.sql", "r");
    char line[1000];
    int i = 0;
    int isnormalquery = 0;
    while (fscanf(fin, "%d\n", &isnormalquery) && fgets(line, 1000, fin) != NULL) {
        i++;

        char sqlname[100];
		sprintf(sqlname, "normal/%d.sql", i);
		fout = fopen(sqlname, "w");
    	fprintf(fout, "set session role=usertest%d;\n", i);
    	fprintf(fout, "%s\n", line);
    	fclose(fout);

    	{
    	char sqlname[100];
		sprintf(sqlname, "../ans/normal%d_fail.ans", i);
		fout = fopen(sqlname, "w");
		fclose(fout);
    	}

    	{
		char sqlname[100];
		sprintf(sqlname, "../ans/super%d_fail.ans", i);
		fout = fopen(sqlname, "w");
		fclose(fout);
		}

    	{
		char sqlname[100];
		sprintf(sqlname, "../ans/normal%d_success.ans", i);
		fout = fopen(sqlname, "w");
		fclose(fout);
		}

		{
		char sqlname[100];
		sprintf(sqlname, "../ans/super%d_success.ans", i);
		fout = fopen(sqlname, "w");
		fclose(fout);
		}
    	{
    	    	char sqlname[100];
    			sprintf(sqlname, "../ans/admin%d.ans", i);
    			fout = fopen(sqlname, "w");
    			fclose(fout);
    	    	}



        if (!isnormalquery){
        	char sqlnamesuper[100];
			sprintf(sqlnamesuper, "super/%d.sql", i);
			foutsuper = fopen(sqlnamesuper, "w");
			fprintf(foutsuper, "set session role=usersuper%d;\n", i);
			fprintf(foutsuper, "%s\n", line);
			fclose(foutsuper);
        }

        char sqlnameadmin[100];
		sprintf(sqlnameadmin, "admin/%d.sql", i);
		fout = fopen(sqlnameadmin, "w");
		fprintf(fout, "%s\n", line);
		fclose(fout);
    }
    fclose(fin);
}

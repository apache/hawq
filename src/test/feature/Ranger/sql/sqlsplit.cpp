#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
    FILE *fin, *fout, *foutsuper;
    fin = fopen("sqlsplitinput", "r");
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

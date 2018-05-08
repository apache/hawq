## How to create a vectorized type in HAWQ##

For the original version of HAWQ vectorized execution, it only supports six type in **[vtype.h/c]** for vectorized execution. The *README* file represents lots of information relevant to custom your own type for vectorized execution. However, if you still struggle with the coding process, this document may help you pave the way to familiar with type customization. We will provide an example as follow. The *date* type is chosen as a *vtype* internal to build a new vectorized execution feature, and all detail and practice experiences are presented in this doc step by step.  You can find the type and relevant expression function implementation in **[vtype_ext.h/c]**.

###I. Vectorizable type ###

Before a new vectorized type implementation initiated, an original type must be selected, which is named as *internal type* in this example. At present, the internal type length must smaller than *Datum*. The reason is that reference depended type serialization methods are not supported, which may require huge amounts of memory resource if support those in vectorized execution. Consequently, if you want to patch this feature, you should do some further workload, including refactor serialization function and manage type required memory manually. Since the *date* type satisfies the requirement, and frequently appear in database case, we extend it to the vectorized version named *vdateadt*.



### II. Type defination in SQL script###

For type definition, we normally execute four SQL querys as follow:

```sql
CREATE TYPE vdateadt;
CREATE FUNCTION vdateadtin(cstring) RETURNS vdateadt AS 'vexecutor.so' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION vdateadtout(vdateadt) RETURNS cstring AS 'vexecutor.so' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE vdateadt ( INPUT = vdateadtin, OUTPUT = vdateadtout, element = date , storage=external);
```

The function *vdateadtin* and *vdateadtout* is the input and output casting function for the *vdateadt* type separately. The declare script include the function formal parameter and return type, as well as specific library name which HAWQ can find the function. When the declaration query has been executed, the new type metadata is recorded in HAWQ ***pg_type*** catalog table.  You can query the detail using ```SELECT * FROM pg_type WHERE typname='vdateadt';```

Secondly, the new type related expression function need to declare. For *date* type, it holds  *date_mi* function to process date minus date expression. As the affine function in *vdateadt*, we declare function *vdataadt_mi* to handle *vdateadt* - *vdateadt* calculation. However, for some queries (e.g. ```SELECT col1 from tab1 WHERE vdate - '1998-08-02' > 20```), the *vdateadt* - *date* expression is required. Since that, we declare another function named *vdateadt_mi_date*. These two function declaration query is posted as follow:

```sql
CREATE FUNCTION vdateadt_mi(vdateadt, vdateadt) RETURNS vint4 AS 'vexecutor.so' LANGUAGE C IMMUTABLE STRICT;
CREATE OPERATOR - ( leftarg = vdateadt, rightarg = vdateadt, procedure = vdateadt_mi, commutator = - );
CREATE OPERATOR - ( leftarg = vdateadt, rightarg = date, procedure = vdateadt_mi_dateadt, commutator = - );
CREATE FUNCTION vdateadt_pli_int4(vdateadt, int4) RETURNS vdateadt AS 'vexecutor.so' LANGUAGE C IMMUTABLE STRICT;
```

Like the case we show above, all the expression declaration is wrote in *create_type.sql*.



###III. Type implementation  ###

As the declaration notice, all function should define and implement in library *vexecutor*. The header file of *vtype_ext.h* is included in *vexecutor.h* and *vcheck.h*. Thus, HAWQ query dispatcher and library can recognize the *date* type vectorizable.

For example, *vdateadt_pli_int4* has been defined as follow.

```c
PG_FUNCTION_INFO_V1(vdateadt_pli_int4);
Datum vdateadt_pli_int4(PG_FUNCTION_ARGS)
{
    int size = 0;
    int i = 0;
    vdateadt* arg1 = PG_GETARG_POINTER(0);
    int arg2 = PG_GETARG_INT32(1);
    vdateadt* res = buildvint4(BATCHSIZE, NULL);
    size = arg1->dim;
    while(i < size)
    {
        res->isnull[i] = arg1->isnull[i] ;
        if(!res->isnull[i])
            res->values[i] = Int32GetDatum((DatumGetDateADT(arg1->values[i])) + arg2);
        i++;
    }
    res->dim = arg1->dim;
    PG_RETURN_POINTER(res);
}
```

Depends on the SQL script declare in the previous step. HAWQ dispatch two parameters into when the function is invoked. The MARCO *PG_FUNCTION_INFO_V1* notice the compiler how to compile and load the function into the system. *isnull* attribute is an array of *bool* to indicate every internal slot if is null. The *dim* attribute indicates the number of values which contain in the *vtype*.

All expression functions are defined in *vtype_ext.c*. For convenience, you can use MARCO function to implement, since most calculation functions logic are same but operator different.

### IV. Install###

After finish coding, compile and install the library first. HAWQ require restart to recognize new thrid-part library load. the library name should record into *local_preload_libraries* parameter *postgresql.conf* file both master and segment data directories.

Executing ```\i create_type.sql``` all vecotrized type will be created in HAWQ, and open *vectorized_executor_enable* GUC value to on trigger **VE** work.

###V. Test###

We are glad to accept new feature into HAWQ. If you complete a new type or has some enlightenment job, I appreciate it and you can create a PR in HAWQ project in GitHub. We will review and merge it if it is helpful and bug-free.

However, before publishing your code out, you should test your coding in our test framework. For the *VE* feature test, it is located in **src/test/feature/vexecutor** directory. Add your own test case and proof everything right.



All in all, this is a brief introduction for vtype extension. 
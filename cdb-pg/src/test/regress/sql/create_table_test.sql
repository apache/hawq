--
-- COPY
--

CREATE TABLE aggtest (
    a           int2,
    b           float4
);

CREATE TABLE tenk1 (
    unique1     int4,
    unique2     int4,
    two         int4,
    four        int4,
    ten         int4,
    twenty      int4,
    hundred     int4,
    thousand    int4,
    twothousand int4,
    fivethous   int4,
    tenthous    int4,
    odd         int4,
    even        int4,
    stringu1    name,
    stringu2    name,
    string4     name
) WITH OIDS;

CREATE TABLE slow_emp4000 (
    home_base    box
);

CREATE TABLE person (
    name        text,
    age         int4,
    location    point
);

CREATE TABLE onek (
    unique1     int4,
    unique2     int4,
    two         int4,
    four        int4,
    ten         int4,
    twenty      int4,
    hundred     int4,
    thousand    int4,
    twothousand int4,
    fivethous   int4,
    tenthous    int4,
    odd         int4,
    even        int4,
    stringu1    name,
    stringu2    name,
    string4     name
);

CREATE TABLE emp (
    salary      int4,
    manager     name
) INHERITS (person) WITH OIDS;


CREATE TABLE student (
    gpa         float8
) INHERITS (person);


CREATE TABLE stud_emp (
    percent     int4
) INHERITS (emp, student);

CREATE TABLE real_city (
    pop         int4,
    cname       text,
    outline     path
);

CREATE TABLE road (
    name        text,
    thepath     path
);

CREATE TABLE hash_i4_heap (
    seqno       int4,
    random      int4
);

CREATE TABLE hash_name_heap (
    seqno       int4,
    random      name
);

CREATE TABLE hash_txt_heap (
    seqno       int4,
    random      text
);

CREATE TABLE hash_f8_heap (
    seqno       int4,
    random      float8
);

CREATE TABLE bt_i4_heap (
    seqno       int4,
    random      int4
);

CREATE TABLE bt_name_heap (
    seqno       name,
    random      int4
);

CREATE TABLE bt_txt_heap (
    seqno       text,
    random      int4
);

CREATE TABLE bt_f8_heap (
    seqno       float8,
    random      int4
);

CREATE TABLE array_op_test (
    seqno       int4,
    i           int4[],
    t           text[]
);

CREATE TABLE array_index_op_test (
    seqno       int4,
    i           int4[],
    t           text[]
);

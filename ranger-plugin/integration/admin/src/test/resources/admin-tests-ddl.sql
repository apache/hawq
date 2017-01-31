-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- EAST Database and its objects
DROP DATABASE IF EXISTS east;
CREATE DATABASE east;
\c east;
CREATE SCHEMA common;
CREATE TABLE common.rice (id integer);
CREATE TABLE common.soup (id integer);
CREATE SEQUENCE common.water;
CREATE SEQUENCE common.sprite;
CREATE FUNCTION common.eat(integer) RETURNS integer AS 'select $1;' LANGUAGE SQL;
CREATE FUNCTION common.sleep(integer) RETURNS integer AS 'select $1;' LANGUAGE SQL;
CREATE SCHEMA japan;
CREATE TABLE japan.rice (id integer);
CREATE TABLE japan.sushi (id integer);
CREATE SEQUENCE japan.water;
CREATE SEQUENCE japan.sake;
CREATE FUNCTION japan.eat(integer) RETURNS integer AS 'select $1;' LANGUAGE SQL;
CREATE FUNCTION japan.stand(integer) RETURNS integer AS 'select $1;' LANGUAGE SQL;
CREATE LANGUAGE langdbeast HANDLER plpgsql_call_handler;

-- WEST Database and its objects
DROP DATABASE IF EXISTS west;
CREATE DATABASE west;
\c west;
CREATE SCHEMA common;
CREATE TABLE common.rice (id integer);
CREATE TABLE common.soup (id integer);
CREATE SEQUENCE common.water;
CREATE SEQUENCE common.sprite;
CREATE FUNCTION common.eat(integer) RETURNS integer AS 'select $1;' LANGUAGE SQL;
CREATE FUNCTION common.sleep(integer) RETURNS integer AS 'select $1;' LANGUAGE SQL;
CREATE SCHEMA france;
CREATE TABLE france.rice (id integer);
CREATE TABLE france.stew (id integer);
CREATE SEQUENCE france.water;
CREATE SEQUENCE france.scotch;
CREATE FUNCTION france.eat(integer) RETURNS integer AS 'select $1;' LANGUAGE SQL;
CREATE FUNCTION france.smile(integer) RETURNS integer AS 'select $1;' LANGUAGE SQL;
CREATE LANGUAGE langdbwest HANDLER plpgsql_call_handler;
CREATE SCHEMA jamaica;

-- Database without an explicit schema
DROP DATABASE IF EXISTS noschema_db;
CREATE DATABASE noschema_db;
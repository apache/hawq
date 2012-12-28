CREATE TABLE toastable_ao(a text, b varchar, c int) with(appendonly=true, compresslevel=1) distributed randomly;

-- INSERT 
-- uses the toast call to store the large tuples
INSERT INTO toastable_ao VALUES(repeat('a',100000), repeat('b',100001), 1);
INSERT INTO toastable_ao VALUES(repeat('A',100000), repeat('B',100001), 2);

-- Check that tuples were toasted and are detoasted correctly. we use
-- char_length() because it guarantees a detoast without showing tho whole result
SELECT char_length(a), char_length(b), c FROM toastable_ao ORDER BY c;

-- ALTER
-- this will cause a full table rewrite. we make sure the tosted values and references
-- stay intact after all the oid switching business going on.
-- ALTER TABLE toastable_ao ADD COLUMN d int DEFAULT 10;

-- SELECT char_length(a), char_length(b), c, d FROM toastable_ao ORDER BY c;
SELECT char_length(a), char_length(b), c FROM toastable_ao ORDER BY c;

-- TRUNCATE
-- remove reference to toast table and create a new one with different values
TRUNCATE toastable_ao;

INSERT INTO toastable_ao VALUES(repeat('a',100002), repeat('b',100003), 2);

SELECT char_length(a), char_length(b), c FROM toastable_ao;

-- TODO: figure out a way to verify that toasted data is removed after the truncate.

DROP TABLE toastable_ao;

-- TODO: figure out a way to verify that the toast tables are dropped

CREATE OPERATOR CLASS sva_special_ops FOR TYPE text USING btree AS OPERATOR 1 <#, FUNCTION 1 si_same(text, text);


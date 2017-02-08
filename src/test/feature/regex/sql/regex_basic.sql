--
-- Regular expression tests
--

-- Don't want to have to double backslashes in regexes
set standard_conforming_strings = on;

-- Test simple quantified backrefs
select 'bbbbb' ~ '^([bc])\1*$' as t;
select 'ccc' ~ '^([bc])\1*$' as t;
select 'xxx' ~ '^([bc])\1*$' as f;
select 'b' ~ '^([bc])\1*$' as t;

-- Test lookahead constraints
select regexp_matches('ab', 'a(?=b)b*');
select regexp_matches('a', 'a(?=b)b*');
select regexp_matches('abc', 'a(?=b)b*(?=c)c*');
select regexp_matches('ab', 'a(?=b)b*(?=c)c*');
select regexp_matches('ab', 'a(?!b)b*');
select regexp_matches('a', 'a(?!b)b*');
select regexp_matches('b', '(?=b)b');
select regexp_matches('a', '(?=b)b');

-- Test optimization of single-chr-or-bracket-expression lookaround constraints
select 'xz' ~ 'x(?=[xy])';
select 'xy' ~ 'x(?=[xy])';
select 'xz' ~ 'x(?![xy])';
select 'xy' ~ 'x(?![xy])';
select 'x'  ~ 'x(?![xy])';
select 'zyy' ~ '(?<![xy])yy+';

-- Test for infinite loop in cfindloop with zero-length possible match
-- but no actual match (can only happen in the presence of backrefs)
select 'a' ~ '$()|^\1';
select 'a' ~ '.. ()|\1';
select 'a' ~ '()*\1';
select 'a' ~ '()+\1';

-- Error conditions
select 'xyz' ~ 'x(\w)(?=\1)';  -- no backrefs in LACONs
select 'a' ~ '\x7fffffff';  -- invalid chr code

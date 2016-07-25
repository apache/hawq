CREATE OR REPLACE FUNCTION plperl_max (INTEGER, INTEGER)
RETURNS INTEGER
AS $$
    my ($x, $y) = @_;
    if (not defined $x) {
        return undef if not defined $y;
        return $y;
    }
    return $x if not defined $y;
    return $x if $x > $y;
    return $y;
$$ LANGUAGE plperl;

SELECT plperl_max(1, 10);

CREATE OR REPLACE FUNCTION plperl_returns_array()
RETURNS TEXT[][]
AS $$
    return [['a"b','c,d'],['e\\f','g']];
$$ LANGUAGE plperl;

SELECT plperl_returns_array();

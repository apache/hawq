-- test plperl triggers

CREATE TYPE rowcomp as (i int);
CREATE TYPE rowcompnest as (rfoo rowcomp);
CREATE TABLE trigger_test (
        i int,
        v varchar,
		foo rowcompnest
) distributed by (i);

CREATE OR REPLACE FUNCTION trigger_data() RETURNS trigger LANGUAGE plperl AS $$

  # make sure keys are sorted for consistent results - perl no longer
  # hashes in  repeatable fashion across runs

  sub str {
	  my $val = shift;

	  if (!defined $val)
	  {
		  return 'NULL';
	  }
	  elsif (ref $val eq 'HASH')
	  {
		my $str = '';
		foreach my $rowkey (sort keys %$val)
		{
		  $str .= ", " if $str;
		  my $rowval = str($val->{$rowkey});
		  $str .= "'$rowkey' => $rowval";
		}
		return '{'. $str .'}';
	  }
	  elsif (ref $val eq 'ARRAY')
	  {
		  my $str = '';
		  for my $argval (@$val)
		  {
			  $str .= ", " if $str;
			  $str .= str($argval);
		  }
		  return '['. $str .']';
	  }
	  else
	  {
		  return "'$val'";
	  }
  }

  foreach my $key (sort keys %$_TD)
  {

    my $val = $_TD->{$key};

	# relid is variable, so we can not use it repeatably
	$val = "bogus:12345" if $key eq 'relid';

	elog(NOTICE, "\$_TD->\{$key\} = ". str($val));
  }
  return undef; # allow statement to proceed;
$$;

CREATE TRIGGER show_trigger_data_trig
BEFORE INSERT OR UPDATE OR DELETE ON trigger_test
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

insert into trigger_test values(1,'insert', '("(1)")');
update trigger_test set v = 'update' where i = 1;
delete from trigger_test;

DROP TRIGGER show_trigger_data_trig on trigger_test;

DROP FUNCTION trigger_data();

CREATE OR REPLACE FUNCTION valid_id() RETURNS trigger AS $$

    if (($_TD->{new}{i}>=100) || ($_TD->{new}{i}<=0))
    {
        return "SKIP";   # Skip INSERT/UPDATE command
    }
    elsif ($_TD->{new}{v} ne "immortal")
    {
        $_TD->{new}{v} .= "(modified by trigger)";
		$_TD->{new}{foo}{rfoo}{i}++;
        return "MODIFY"; # Modify tuple and proceed INSERT/UPDATE command
    }
    else
    {
        return;          # Proceed INSERT/UPDATE command
    }
$$ LANGUAGE plperl;

CREATE TRIGGER "test_valid_id_trig" BEFORE INSERT OR UPDATE ON trigger_test
FOR EACH ROW EXECUTE PROCEDURE "valid_id"();

INSERT INTO trigger_test (i, v, foo) VALUES (1,'first line', '("(1)")');
INSERT INTO trigger_test (i, v, foo) VALUES (2,'second line', '("(2)")');
INSERT INTO trigger_test (i, v, foo) VALUES (3,'third line', '("(3)")');
INSERT INTO trigger_test (i, v, foo) VALUES (4,'immortal', '("(4)")');

INSERT INTO trigger_test (i, v) VALUES (101,'bad id');

SELECT * FROM trigger_test;

UPDATE trigger_test SET i = 5 where i=3;

UPDATE trigger_test SET i = 100 where i=1;

SELECT * FROM trigger_test;

CREATE OR REPLACE FUNCTION immortal() RETURNS trigger AS $$
    if ($_TD->{old}{v} eq $_TD->{args}[0])
    {
        return "SKIP"; # Skip DELETE command
    }
    else
    {
        return;        # Proceed DELETE command
    };
$$ LANGUAGE plperl;

CREATE TRIGGER "immortal_trig" BEFORE DELETE ON trigger_test
FOR EACH ROW EXECUTE PROCEDURE immortal('immortal');

DELETE FROM trigger_test;

SELECT * FROM trigger_test;

CREATE FUNCTION direct_trigger() RETURNS trigger AS $$
    return;
$$ LANGUAGE plperl;

SELECT direct_trigger();

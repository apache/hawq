set session role=usertest57;
begin; DECLARE mycursor CURSOR FOR SELECT * FROM a order by i; FETCH FORWARD 2 FROM mycursor; commit;


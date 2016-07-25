SELECT set_config('pljava_classpath', 'PLJavauAdd.jar', false);

CREATE OR REPLACE FUNCTION pljavau_add(x INT, y INT)
RETURNS INT
AS 'PLJavauAdd.add'
LANGUAGE javau;

SELECT pljavau_add(10, 20);

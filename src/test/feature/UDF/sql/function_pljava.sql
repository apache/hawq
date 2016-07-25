SELECT set_config('pljava_classpath', 'PLJavaAdd.jar', false);

CREATE OR REPLACE FUNCTION pljava_add(x INT, y INT)
RETURNS INT
AS 'PLJavaAdd.add'
LANGUAGE java;

SELECT pljava_add(10, 20);

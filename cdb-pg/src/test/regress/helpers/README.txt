Avro Producers
--------------

AvroFile producers
CustomAvroFile.java
CustomAvroRecInFile.java

Avro inside SequenceFile
CustomAvroSequence.java
CustomAvroRecInSequence.java

These will use ../data/pxf/regressPXFCustomAvro.avsc as schema
(parameter on Makefile)

To create an AvroFile
make CustomAvroFile

To create an Avro inside SequenceFile
make CustomAvroSequence

Output will be written to avro_inside_sequence.tbl
and avroformat_inside_avrofile.avro
To use them, copy them over to ../data/pxf

Make sure your environment contains the following:
GPHD_ROOT where you have PXF and Hadoop

CustomWritable Producer
-----------------------

CustomWritable.java
CustomWritableSequence.java

To create the SequenceFile
make CustomWritableSequence

Output will be written to writable_inside_sequece.tbl
To use it, copy CustomWritable.class and 
writable_inside_sequece.tbl to ../data/pxf

Make sure your environment contains the following:
GPHD_ROOT where you have PXF and Hadoop

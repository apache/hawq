PXF_ROOT=$(GPHD_ROOT)/pxf
HADOOP_ROOT=$(GPHD_ROOT)/hadoop
HADOOPCOMMON_ROOT=$(HADOOP_ROOT)/share/hadoop/common
HADOOPCOMMONLIB_ROOT=$(HADOOP_ROOT)/share/hadoop/common/lib
HADOOPMAPREDUCE_ROOT=$(HADOOP_ROOT)/share/hadoop/mapreduce

CLASSPATH_list=\
			   $(PXF_ROOT)/*\
			   $(HADOOPCOMMON_ROOT)/*\
			   $(HADOOPCOMMONLIB_ROOT)/*\
			   $(HADOOPMAPREDUCE_ROOT)/*\


CLASSPATH=$(subst : ,:,$(strip $(foreach j, $(CLASSPATH_list), $j:)))

%.class: %.java
	javac -cp $(CLASSPATH) $^

CustomAvroSequence: CustomAvroSequence.class CustomAvroRecInSequence.class
	java -cp $(CLASSPATH) $@ . avro_inside_sequence.tbl 1 ../data/pxf/regressPXFCustomAvro.avsc

CustomAvroFile: CustomAvroFile.class CustomAvroRecInFile.class
	java -cp $(CLASSPATH) $@ . avroformat_inside_avrofile.avro 1 ../data/pxf/regressPXFCustomAvro.avsc

CustomWritableSequence: CustomWritable.class CustomWritableSequence.class
	java -cp $(CLASSPATH) $@ . writable_inside_sequece.tbl 1

clean:
	rm *.class
	rm *~
	rm avro_inside_sequence.tbl
	rm avroformat_inside_avrofile.avro
	rm writable_inside_sequece.tbl

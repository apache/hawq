package org.apache.hawq.plugins.json;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.security.InvalidParameterException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonInputFormat extends FileInputFormat<Text, NullWritable> {

	private static JsonFactory factory = new JsonFactory();
	private static ObjectMapper mapper = new ObjectMapper(factory);

	public static final String ONE_RECORD_PER_LINE = "json.input.format.one.record.per.line";
	public static final String RECORD_IDENTIFIER = "json.input.format.record.identifier";

	@Override
	public RecordReader<Text, NullWritable> getRecordReader(InputSplit split,
			JobConf conf, Reporter reporter) throws IOException {

		if (conf.getBoolean(ONE_RECORD_PER_LINE, false)) {

			return new SimpleJsonRecordReader(conf, (FileSplit) split);
		} else {
			return new JsonRecordReader(conf, (FileSplit) split);
		}
	}

	public static class SimpleJsonRecordReader implements
			RecordReader<Text, NullWritable> {

		private LineRecordReader rdr = null;
		private LongWritable key = new LongWritable();
		private Text value = new Text();

		public SimpleJsonRecordReader(Configuration conf, FileSplit split)
				throws IOException {
			rdr = new LineRecordReader(conf, split);
		}

		@Override
		public void close() throws IOException {
			rdr.close();
		}

		@Override
		public Text createKey() {
			return value;
		}

		@Override
		public NullWritable createValue() {
			return NullWritable.get();
		}

		@Override
		public long getPos() throws IOException {
			return rdr.getPos();
		}

		@Override
		public boolean next(Text key, NullWritable value) throws IOException {
			if (rdr.next(this.key, this.value)) {
				key.set(this.value);
				return true;
			} else {
				return false;
			}
		}

		@Override
		public float getProgress() throws IOException {
			return rdr.getProgress();
		}
	}

	public static class JsonRecordReader implements
			RecordReader<Text, NullWritable> {

		private Logger LOG = Logger.getLogger(JsonRecordReader.class);

		private JsonStreamReader rdr = null;
		private long start = 0, end = 0;
		private float toRead = 0;
		private String identifier = null;
		private Logger log = Logger.getLogger(JsonRecordReader.class);

		public JsonRecordReader(JobConf conf, FileSplit split)
				throws IOException {
			log.info("JsonRecordReader constructor called.  Conf is " + conf
					+ ". Split is " + split);
			this.identifier = conf.get(RECORD_IDENTIFIER);
			log.info("Identifier is " + this.identifier);

			if (this.identifier == null || identifier.isEmpty()) {
				throw new InvalidParameterException(
						JsonInputFormat.RECORD_IDENTIFIER + " is not set.");
			} else {
				LOG.info("Initializing JsonRecordReader with identifier "
						+ identifier);
			}

			// get relevant data
			Path file = split.getPath();

			log.info("File is " + file);

			start = split.getStart();
			end = start + split.getLength();
			toRead = end - start;
			log.info("FileSystem is " + FileSystem.get(conf));

			FSDataInputStream strm = FileSystem.get(conf).open(file);

			log.info("Retrieved file stream ");

			if (start != 0) {
				strm.seek(start);
			}

			rdr = new JsonStreamReader(identifier,
					new BufferedInputStream(strm));

			log.info("Reader is " + rdr);
		}

		@Override
		public boolean next(Text key, NullWritable value) throws IOException {

			boolean retval = false;
			boolean keepGoing = false;
			do {
				// Exit condition (end of block/file)
				if (rdr.getBytesRead() >= (end - start)) {
					return false;
				}

				keepGoing = false;
				String record = rdr.getJsonRecord();
				if (record != null) {
					if (JsonInputFormat.decodeLineToJsonNode(record) == null) {
						log.error("Unable to parse JSON string.  Skipping. DEBUG to see");
						log.debug(record);
						keepGoing = true;
					} else {
						key.set(record);
						retval = true;
					}
				}
			} while (keepGoing);

			return retval;
		}

		@Override
		public Text createKey() {
			return new Text();
		}

		@Override
		public NullWritable createValue() {
			return NullWritable.get();
		}

		@Override
		public long getPos() throws IOException {
			return start + rdr.getBytesRead();
		}

		@Override
		public void close() throws IOException {
			rdr.close();
		}

		@Override
		public float getProgress() throws IOException {
			return (float) rdr.getBytesRead() / toRead;
		}
	}

	public static synchronized JsonNode decodeLineToJsonNode(String line) {

		try {
			return mapper.readTree(line);
		} catch (JsonParseException e) {
			e.printStackTrace();
			return null;
		} catch (JsonMappingException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
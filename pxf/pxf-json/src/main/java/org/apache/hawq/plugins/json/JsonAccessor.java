package org.apache.hawq.plugins.json;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hdfs.HdfsSplittableDataAccessor;


/**
 * This JSON accessor for PXF will read JSON data and pass it to a
 * {@link JsonResolver}.
 * 
 * This accessor supports a single JSON record per line, or a more
 * "pretty print" format.
 */
public class JsonAccessor extends HdfsSplittableDataAccessor {

	public static final String IDENTIFIER_PARAM = "IDENTIFIER";
	public static final String ONERECORDPERLINE_PARAM = "ONERECORDPERLINE";

	private String identifier = "";
	private boolean oneRecordPerLine = true;

	public JsonAccessor(InputData inputData) throws Exception {
		super(inputData, new JsonInputFormat());

		if (!StringUtils.isEmpty(inputData.getUserProperty(IDENTIFIER_PARAM))) {
			identifier = inputData.getUserProperty(IDENTIFIER_PARAM);
		}

		if (!StringUtils.isEmpty(inputData.getUserProperty(ONERECORDPERLINE_PARAM))) {
			oneRecordPerLine = Boolean.parseBoolean(inputData.getUserProperty(ONERECORDPERLINE_PARAM));
		}
	}

	@Override
	protected Object getReader(JobConf conf, InputSplit split)
			throws IOException {
		conf.set(JsonInputFormat.RECORD_IDENTIFIER, identifier);

		if (oneRecordPerLine) {
			return new JsonInputFormat.SimpleJsonRecordReader(conf,
					(FileSplit) split);
		} else {
			return new JsonInputFormat.JsonRecordReader(conf, (FileSplit) split);
		}
	}
}
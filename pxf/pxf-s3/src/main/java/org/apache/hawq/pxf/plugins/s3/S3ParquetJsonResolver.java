package org.apache.hawq.pxf.plugins.s3;

import java.util.ArrayList;
import java.util.List;

import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadResolver;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

public class S3ParquetJsonResolver extends Plugin implements ReadResolver {

	public S3ParquetJsonResolver(InputData input) {
		super(input);
	}

	@Override
	public List<OneField> getFields(OneRow row) throws Exception {
		List<OneField> record = new ArrayList<>();
		record.add(new OneField(DataType.VARCHAR.getOID(), row.getData()));
		return record;
	}

}

package com.pxf.infra.structures.tables.pxf;

import com.pxf.infra.structures.tables.basic.Table;

/**
 * Represent HAWQ -> PXF external tabel.
 */
public abstract class ExternalTable extends Table {

	private String hostname = "localhost";

	private String port = "50070";

	private String path = "somepath/gpdb_regression_data";

	private String fragmenter;

	private String accessor;

	private String resolver;

	private String dataSchema;

	private String format;

	private String formatter;

	private String delimiter;

	private String[] userParamters;

	private String profile;

	private int segmentRejectLimit = 0;

	private String analyzer;

	public ExternalTable(String name, String[] fields, String path, String format) {
		super(name, fields);
		setPath(path);
		setFormat(format);
	}

	public String getFragmenter() {
		return fragmenter;
	}

	public void setFragmenter(String fragmenter) {
		this.fragmenter = fragmenter;
	}

	public String getAccessor() {
		return accessor;
	}

	public void setAccessor(String accessor) {
		this.accessor = accessor;
	}

	public String getResolver() {
		return resolver;
	}

	public void setResolver(String resolver) {
		this.resolver = resolver;
	}

	public String getDataSchema() {
		return dataSchema;
	}

	public void setDataSchema(String dataSchema) {
		this.dataSchema = dataSchema;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getFormatter() {
		return formatter;
	}

	public void setFormatter(String formatter) {
		this.formatter = formatter;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String[] getUserParamters() {
		return userParamters;
	}

	public void setUserParamters(String[] userParamters) {
		this.userParamters = userParamters;
	}

	@Override
	public String constructDropStmt() {

		StringBuilder sb = new StringBuilder();

		sb.append("DROP EXTERNAL TABLE IF EXISTS " + getFullName());

		return sb.toString();
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getProfile() {
		return profile;
	}

	public void setProfile(String profile) {
		this.profile = profile;
	}

	public int getSegmentRejectLimit() {
		return segmentRejectLimit;
	}

	public void setSegmentRejectLimit(int segmentRejectLimit) {
		this.segmentRejectLimit = segmentRejectLimit;
	}

	@Override
	protected String createHeader() {
		return "CREATE EXTERNAL TABLE " + getFullName() + " ";
	}

	@Override
	protected String createLocation() {

		StringBuilder sb = new StringBuilder();

		sb.append("LOCATION ('pxf://" + getHostname() + ":" + getPort() + "/" + getPath() + "?");

		sb.append(getLocationParameters());

		sb.append("')");

		return sb.toString();
	}

	/**
	 * generates location for create query
	 * 
	 * @return
	 */
	protected String getLocationParameters() {

		StringBuilder sb = new StringBuilder();

		if (getProfile() != null) {

			sb.append("PROFILE=" + getProfile());

		} else {
			if (getFragmenter() != null) {

				sb.append("FRAGMENTER=" + getFragmenter());

				if (getAccessor() != null) {
					sb.append("&ACCESSOR=" + getAccessor());
				}

				if (getResolver() != null) {

					sb.append("&RESOLVER=" + getResolver());
				}

			} else if (getAccessor() != null) {

				sb.append("ACCESSOR=" + getAccessor());

				if (getResolver() != null) {

					sb.append("&RESOLVER=" + getResolver());
				}

			} else if (getResolver() != null) {

				sb.append("RESOLVER=" + getResolver());
			}
		}

		if (getAnalyzer() != null) {
			sb.append("&ANALYZER=" + getAnalyzer());
		}

		if (getDataSchema() != null) {

			sb.append("&DATA-SCHEMA=" + getDataSchema());
		}

		if (getUserParamters() != null) {

			if (sb.length() > 0) {
				sb.append("&");
			}

			for (int i = 0; i < getUserParamters().length; i++) {
				sb.append(getUserParamters()[i]);

				if (i != getUserParamters().length - 1) {
					sb.append("&");
				}
			}
		}

		return sb.toString();
	}

	@Override
	public String constructCreateStmt() {
		StringBuilder sb = new StringBuilder();

		sb.append(createHeader() + " ");
		sb.append(createFields() + " ");
		sb.append(createLocation() + " ");

		if (getFormat() != null) {
			sb.append(" FORMAT '" + getFormat() + "' ");

		}

		if (getFormatter() != null) {
			sb.append("(formatter='" + getFormatter() + "')");
		}

		if (getDelimiter() != null) {
			sb.append("(DELIMITER '" + getDelimiter() + "')");
		}

		if (getSegmentRejectLimit() > 0) {
			sb.append(" SEGMENT REJECT LIMIT " + getSegmentRejectLimit());
		}

		return sb.toString();
	}

	public String getAnalyzer() {
		return analyzer;
	}

	public void setAnalyzer(String analyzer) {
		this.analyzer = analyzer;
	}
}

package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.threeglav.sh.bauk.util.StringUtil;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class Feed {

	@XmlAttribute(required = true)
	private String name;

	@XmlElement(required = true)
	private FeedSource source;

	@XmlElement(required = true)
	private String archiveDirectory;

	@XmlElement(required = true)
	private String bulkOutputDirectory;

	@XmlElement(required = true)
	private String errorDirectory;

	@XmlElement(required = false)
	private String fileNameProcessorClassName;

	@XmlAttribute(required = true)
	private FeedType type;

	@XmlElement(required = false, defaultValue = "-1")
	private Integer repetitionCount = -1;

	@XmlElement
	private String nullString;

	@XmlElement
	private String delimiterString;

	@XmlElement
	private Header header;

	@XmlElement
	private Data data;

	@XmlElement
	private Footer footer;

	@XmlElement
	private BulkLoadDefinition bulkLoadDefinition;

	@XmlElement
	private ThreadPoolSettings threadPoolSettings;

	@XmlElement
	private FeedEvents events;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public FeedType getType() {
		return type;
	}

	public void setType(final FeedType type) {
		this.type = type;
	}

	public String getNullString() {
		return nullString;
	}

	public void setNullString(final String nullString) {
		this.nullString = nullString;
	}

	public String getDelimiterString() {
		return delimiterString;
	}

	public void setDelimiterString(final String delimiterString) {
		this.delimiterString = delimiterString;
	}

	public Header getHeader() {
		return header;
	}

	public void setHeader(final Header header) {
		this.header = header;
	}

	public Footer getFooter() {
		return footer;
	}

	public void setFooter(final Footer footer) {
		this.footer = footer;
	}

	public Data getData() {
		return data;
	}

	public void setData(final Data data) {
		this.data = data;
	}

	public Integer getRepetitionCount() {
		return repetitionCount;
	}

	public void setRepetitionCount(final Integer repetitionCount) {
		this.repetitionCount = repetitionCount;
	}

	public BulkLoadDefinition getBulkLoadDefinition() {
		return bulkLoadDefinition;
	}

	public void setBulkLoadDefinition(final BulkLoadDefinition bulkLoadDefinition) {
		this.bulkLoadDefinition = bulkLoadDefinition;
	}

	public ThreadPoolSettings getThreadPoolSettings() {
		return threadPoolSettings;
	}

	public void setThreadPoolSettings(final ThreadPoolSettings threadPoolSizes) {
		threadPoolSettings = threadPoolSizes;
	}

	public String getFileNameProcessorClassName() {
		return fileNameProcessorClassName;
	}

	public void setFileNameProcessorClassName(final String fileNameProcessorClassName) {
		this.fileNameProcessorClassName = fileNameProcessorClassName;
	}

	public FeedSource getSource() {
		return source;
	}

	public void setSource(final FeedSource source) {
		this.source = source;
	}

	public String getArchiveDirectory() {
		return archiveDirectory;
	}

	public void setArchiveDirectory(final String archiveDirectory) {
		this.archiveDirectory = archiveDirectory;
	}

	public String getBulkOutputDirectory() {
		return bulkOutputDirectory;
	}

	public void setBulkOutputDirectory(final String bulkOutputDirectory) {
		this.bulkOutputDirectory = bulkOutputDirectory;
	}

	public FeedEvents getEvents() {
		return events;
	}

	public void setEvents(final FeedEvents events) {
		this.events = events;
	}

	public String getErrorDirectory() {
		return errorDirectory;
	}

	public void setErrorDirectory(final String errorDirectory) {
		this.errorDirectory = errorDirectory;
	}

	/**
	 * Not represented in xml. Only ETL threads should be running if this is true.
	 * 
	 * @return
	 */
	public boolean isEtlOnlyFactFeed() {
		if (this.getBulkLoadDefinition() != null) {
			final BulkLoadDefinition bld = this.getBulkLoadDefinition();
			final String outputType = bld.getOutputType();
			if (!StringUtil.isEmpty(outputType)) {
				final String outputTypeLower = outputType.toLowerCase();
				return outputTypeLower.equalsIgnoreCase(BulkLoadDefinitionOutputType.JDBC.toString())
						|| outputTypeLower.equalsIgnoreCase(BulkLoadDefinitionOutputType.PIPE.toString());
			}
		}
		return false;
	}

	public int getMinimumRequiredJdbcConnections() {
		if (this.isEtlOnlyFactFeed()) {
			return this.getThreadPoolSettings().getEtlProcessingThreadCount();
		}
		int minCount = 0;
		if (this.getThreadPoolSettings().getDatabaseProcessingThreadCount() > 0) {
			minCount += this.getThreadPoolSettings().getDatabaseProcessingThreadCount();
		}
		if (this.getThreadPoolSettings().getEtlProcessingThreadCount() > 0) {
			minCount += this.getThreadPoolSettings().getEtlProcessingThreadCount();
		}
		return minCount;
	}

}

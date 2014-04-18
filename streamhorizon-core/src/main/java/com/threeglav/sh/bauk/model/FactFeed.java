package com.threeglav.sh.bauk.model;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.threeglav.sh.bauk.util.StringUtil;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class FactFeed {

	@XmlAttribute(required = true)
	private String name;

	@XmlElementWrapper(name = "fileNameMasks")
	@XmlElement(name = "fileNameMask")
	private ArrayList<String> fileNameMasks;

	@XmlElement(required = false)
	private String fileNameProcessorClassName;

	@XmlAttribute(required = true)
	private FactFeedType type;

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

	@XmlElementWrapper(name = "beforeFeedProcessing")
	@XmlElement(name = "sqlStatement")
	private List<MappedResultsSQLStatement> beforeFeedProcessing;

	@XmlElement
	private ThreadPoolSettings threadPoolSettings;

	@XmlElementWrapper(name = "onStartupCommands")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onStartup;

	@XmlElementWrapper(name = "afterFeedProcessingCompletion")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> afterFeedProcessingCompletion;

	@XmlElementWrapper(name = "onFeedProcessingFailure")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onFeedProcessingFailure;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public FactFeedType getType() {
		return type;
	}

	public void setType(final FactFeedType type) {
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

	public ArrayList<String> getFileNameMasks() {
		return fileNameMasks;
	}

	public void setFileNameMasks(final ArrayList<String> fileNameMasks) {
		this.fileNameMasks = fileNameMasks;
	}

	public ThreadPoolSettings getThreadPoolSettings() {
		return threadPoolSettings;
	}

	public void setThreadPoolSettings(final ThreadPoolSettings threadPoolSizes) {
		threadPoolSettings = threadPoolSizes;
	}

	public List<MappedResultsSQLStatement> getBeforeFeedProcessing() {
		return beforeFeedProcessing;
	}

	public void setBeforeFeedProcessing(final List<MappedResultsSQLStatement> beforeFeedProcessing) {
		this.beforeFeedProcessing = beforeFeedProcessing;
	}

	public String getFileNameProcessorClassName() {
		return fileNameProcessorClassName;
	}

	public void setFileNameProcessorClassName(final String fileNameProcessorClassName) {
		this.fileNameProcessorClassName = fileNameProcessorClassName;
	}

	public ArrayList<BaukCommand> getOnStartup() {
		return onStartup;
	}

	public void setOnStartup(final ArrayList<BaukCommand> onStartup) {
		this.onStartup = onStartup;
	}

	public ArrayList<BaukCommand> getAfterFeedProcessingCompletion() {
		return afterFeedProcessingCompletion;
	}

	public void setAfterFeedProcessingCompletion(final ArrayList<BaukCommand> afterFeedProcessingCompletion) {
		this.afterFeedProcessingCompletion = afterFeedProcessingCompletion;
	}

	public ArrayList<BaukCommand> getOnFeedProcessingFailure() {
		return onFeedProcessingFailure;
	}

	public void setOnFeedProcessingFailure(final ArrayList<BaukCommand> onFeedProcessingFailure) {
		this.onFeedProcessingFailure = onFeedProcessingFailure;
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

package com.threeglav.bauk.model;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FactFeed {

	@XmlAttribute(required = true)
	private String name;

	@XmlElementWrapper(name = "fileNameMasks")
	@XmlElement(name = "fileNameMask")
	private ArrayList<String> fileNameMasks;

	@XmlAttribute(required = true)
	private FactFeedType type;

	@XmlElement(required = false)
	private int repetitionCount = -1;

	@XmlElement
	private String nullString;

	@XmlElement
	private String delimiterString;

	@XmlElement
	private HeaderFooter header;

	@XmlElement
	private Data data;

	@XmlElementWrapper
	@XmlElement(name = "attribute")
	private ArrayList<Attribute> derivedAttributes;

	@XmlElement
	private HeaderFooter footer;

	@XmlElement
	private BulkLoadDefinition bulkLoadDefinition;

	@XmlElementWrapper(name = "onFeedProcessingCompletion")
	@XmlElement(name = "sqlStatement")
	private List<String> onFeedProcessingCompletion;

	@XmlElement
	private ThreadPoolSizes threadPoolSizes;

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

	public HeaderFooter getHeader() {
		return header;
	}

	public void setHeader(final HeaderFooter header) {
		this.header = header;
	}

	public HeaderFooter getFooter() {
		return footer;
	}

	public void setFooter(final HeaderFooter footer) {
		this.footer = footer;
	}

	public Data getData() {
		return data;
	}

	public void setData(final Data data) {
		this.data = data;
	}

	public ArrayList<Attribute> getDerivedAttributes() {
		return derivedAttributes;
	}

	public void setDerivedAttributes(final ArrayList<Attribute> derivedAttributes) {
		this.derivedAttributes = derivedAttributes;
	}

	public int getRepetitionCount() {
		return repetitionCount;
	}

	public void setRepetitionCount(final int repetitionCount) {
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

	public ThreadPoolSizes getThreadPoolSizes() {
		return threadPoolSizes;
	}

	public void setThreadPoolSizes(final ThreadPoolSizes threadPoolSizes) {
		this.threadPoolSizes = threadPoolSizes;
	}

	public List<String> getOnFeedProcessingCompletion() {
		return onFeedProcessingCompletion;
	}

	public void setOnFeedProcessingCompletion(final List<String> onFeedProcessingCompletion) {
		this.onFeedProcessingCompletion = onFeedProcessingCompletion;
	}

}

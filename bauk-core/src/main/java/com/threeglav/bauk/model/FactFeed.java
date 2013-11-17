package com.threeglav.bauk.model;

import java.util.ArrayList;

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
	private BulkDefinition bulkDefinition;

	@XmlElement
	private ThreadPoolSizes threadPoolSizes;

	public String getName() {
		return this.name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public FactFeedType getType() {
		return this.type;
	}

	public void setType(final FactFeedType type) {
		this.type = type;
	}

	public String getNullString() {
		return this.nullString;
	}

	public void setNullString(final String nullString) {
		this.nullString = nullString;
	}

	public String getDelimiterString() {
		return this.delimiterString;
	}

	public void setDelimiterString(final String delimiterString) {
		this.delimiterString = delimiterString;
	}

	public HeaderFooter getHeader() {
		return this.header;
	}

	public void setHeader(final HeaderFooter header) {
		this.header = header;
	}

	public HeaderFooter getFooter() {
		return this.footer;
	}

	public void setFooter(final HeaderFooter footer) {
		this.footer = footer;
	}

	public Data getData() {
		return this.data;
	}

	public void setData(final Data data) {
		this.data = data;
	}

	public ArrayList<Attribute> getDerivedAttributes() {
		return this.derivedAttributes;
	}

	public void setDerivedAttributes(final ArrayList<Attribute> derivedAttributes) {
		this.derivedAttributes = derivedAttributes;
	}

	public int getRepetitionCount() {
		return this.repetitionCount;
	}

	public void setRepetitionCount(final int repetitionCount) {
		this.repetitionCount = repetitionCount;
	}

	public BulkDefinition getBulkDefinition() {
		return this.bulkDefinition;
	}

	public void setBulkDefinition(final BulkDefinition bulkDefinition) {
		this.bulkDefinition = bulkDefinition;
	}

	public ArrayList<String> getFileNameMasks() {
		return this.fileNameMasks;
	}

	public void setFileNameMasks(final ArrayList<String> fileNameMasks) {
		this.fileNameMasks = fileNameMasks;
	}

	public ThreadPoolSizes getThreadPoolSizes() {
		return this.threadPoolSizes;
	}

	public void setThreadPoolSizes(final ThreadPoolSizes threadPoolSizes) {
		this.threadPoolSizes = threadPoolSizes;
	}

}

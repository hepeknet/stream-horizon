package com.threeglav.bauk.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class BulkLoadDefinition {

	public static final String DEFAULT_BULK_OUTPUT_VALUE_DELIMITER = ",";

	@XmlElement(required = false)
	private String bulkLoadOutputExtension;

	@XmlElement(required = false, defaultValue = DEFAULT_BULK_OUTPUT_VALUE_DELIMITER)
	private String bulkLoadFileDelimiter = DEFAULT_BULK_OUTPUT_VALUE_DELIMITER;

	@XmlElement(required = false)
	private BulkLoadFormatDefinition bulkLoadFormatDefinition;

	@XmlElementWrapper(name = "afterBulkLoadSuccess")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> afterBulkLoadSuccess;

	@XmlElementWrapper(name = "onBulkLoadFailure")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onBulkLoadFailure;

	@XmlElementWrapper(name = "bulkLoadInsert")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> bulkLoadInsert;

	@XmlAttribute(required = false)
	private BulkLoadDefinitionOutputType outputType = BulkLoadDefinitionOutputType.FILE;

	@XmlElement(name = "output-file-name-pattern", required = false)
	private String outputFileNamePattern;

	public BulkLoadFormatDefinition getBulkLoadFormatDefinition() {
		return bulkLoadFormatDefinition;
	}

	public void setBulkLoadFormatDefinition(final BulkLoadFormatDefinition bulkLoadFormatDefinition) {
		this.bulkLoadFormatDefinition = bulkLoadFormatDefinition;
	}

	public String getBulkLoadOutputExtension() {
		return bulkLoadOutputExtension;
	}

	public void setBulkLoadOutputExtension(final String bulkLoadOutputExtension) {
		this.bulkLoadOutputExtension = bulkLoadOutputExtension;
	}

	public BulkLoadDefinitionOutputType getOutputType() {
		return outputType;
	}

	public void setOutputType(final BulkLoadDefinitionOutputType outputType) {
		this.outputType = outputType;
	}

	public String getOutputFileNamePattern() {
		return outputFileNamePattern;
	}

	public void setOutputFileNamePattern(final String outputFileNamePattern) {
		this.outputFileNamePattern = outputFileNamePattern;
	}

	public String getBulkLoadFileDelimiter() {
		return bulkLoadFileDelimiter;
	}

	public void setBulkLoadFileDelimiter(final String bulkLoadFileDelimiter) {
		this.bulkLoadFileDelimiter = bulkLoadFileDelimiter;
	}

	public ArrayList<BaukCommand> getAfterBulkLoadSuccess() {
		return afterBulkLoadSuccess;
	}

	public void setAfterBulkLoadSuccess(final ArrayList<BaukCommand> afterBulkLoadSuccess) {
		this.afterBulkLoadSuccess = afterBulkLoadSuccess;
	}

	public ArrayList<BaukCommand> getOnBulkLoadFailure() {
		return onBulkLoadFailure;
	}

	public void setOnBulkLoadFailure(final ArrayList<BaukCommand> onBulkLoadFailure) {
		this.onBulkLoadFailure = onBulkLoadFailure;
	}

	public ArrayList<BaukCommand> getBulkLoadInsert() {
		return bulkLoadInsert;
	}

	public void setBulkLoadInsert(final ArrayList<BaukCommand> bulkLoadInsert) {
		this.bulkLoadInsert = bulkLoadInsert;
	}

}

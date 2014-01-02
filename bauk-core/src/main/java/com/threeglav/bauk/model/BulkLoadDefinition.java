package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class BulkLoadDefinition {

	public static final String DEFAULT_BULK_OUTPUT_VALUE_DELIMITER = ",";

	public static final String DEFAULT_BULK_OUTPUT_EXTENSION = "data";

	@XmlElement(required = false, defaultValue = ",")
	private String bulkLoadOutputExtension = DEFAULT_BULK_OUTPUT_EXTENSION;

	@XmlElement(required = false, defaultValue = "data")
	private String bulkLoadFileDelimiter = DEFAULT_BULK_OUTPUT_VALUE_DELIMITER;

	@XmlElement(required = false)
	private BulkLoadFormatDefinition bulkLoadFormatDefinition;

	@XmlElement(required = false)
	private String bulkLoadInsertStatement;

	@XmlElement(required = false)
	private AfterBulkLoadSuccess afterBulkLoadSuccess;

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

	public String getBulkLoadInsertStatement() {
		return bulkLoadInsertStatement;
	}

	public void setBulkLoadInsertStatement(final String bulkLoadInsertStatement) {
		this.bulkLoadInsertStatement = bulkLoadInsertStatement;
	}

	public AfterBulkLoadSuccess getAfterBulkLoadSuccess() {
		return afterBulkLoadSuccess;
	}

	public void setAfterBulkLoadSuccess(final AfterBulkLoadSuccess onBulkLoadSuccess) {
		afterBulkLoadSuccess = onBulkLoadSuccess;
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

}

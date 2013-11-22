package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class BulkLoadDefinition {

	public static final String DEFAULT_BULK_OUTPUT_EXTENSION = "data";

	@XmlElement(required = true)
	private String bulkLoadOutputExtension = DEFAULT_BULK_OUTPUT_EXTENSION;

	@XmlElement(required = true)
	private BulkLoadFormatDefinition bulkLoadFormatDefinition;

	@XmlElement(required = true)
	private String bulkLoadInsertStatement;

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

}

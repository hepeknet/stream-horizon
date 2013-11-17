package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class BulkDefinition {

	public static final String DEFAULT_BULK_OUTPUT_EXTENSION = "data";

	@XmlElement(required = true)
	private String bulkOutputExtension = DEFAULT_BULK_OUTPUT_EXTENSION;

	@XmlElement(required = true)
	private BulkLoadFileDefinition bulkLoadFileDefinition;

	@XmlElement(required = true)
	private String bulkLoadInsertStatement;

	public BulkLoadFileDefinition getBulkLoadFileDefinition() {
		return this.bulkLoadFileDefinition;
	}

	public void setBulkLoadFileDefinition(final BulkLoadFileDefinition bulkLoadFileDefinition) {
		this.bulkLoadFileDefinition = bulkLoadFileDefinition;
	}

	public String getBulkOutputExtension() {
		return this.bulkOutputExtension;
	}

	public void setBulkOutputExtension(final String bulkOutputExtension) {
		this.bulkOutputExtension = bulkOutputExtension;
	}

	public String getBulkLoadInsertStatement() {
		return this.bulkLoadInsertStatement;
	}

	public void setBulkLoadInsertStatement(final String bulkLoadInsertStatement) {
		this.bulkLoadInsertStatement = bulkLoadInsertStatement;
	}

}

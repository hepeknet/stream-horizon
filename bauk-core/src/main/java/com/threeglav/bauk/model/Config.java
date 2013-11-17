package com.threeglav.bauk.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Config {

	@XmlElement(required = true)
	private String sourceDirectory;

	@XmlElement(required = true)
	private String archiveDirectory;

	@XmlElement(required = true)
	private String bulkOutputDirectory;

	@XmlElement(required = true)
	private String errorDirectory;

	@XmlElement(required = true)
	private ConnectionProperties connectionProperties;

	@XmlElementWrapper(required = true)
	@XmlElement(name = "factFeed")
	private ArrayList<FactFeed> factFeeds;

	@XmlElementWrapper(required = true)
	@XmlElement(name = "dimension")
	private ArrayList<Dimension> dimensions;

	public ArrayList<FactFeed> getFactFeeds() {
		return factFeeds;
	}

	public void setFactFeeds(final ArrayList<FactFeed> factFeeds) {
		this.factFeeds = factFeeds;
	}

	public ArrayList<Dimension> getDimensions() {
		return dimensions;
	}

	public void setDimensions(final ArrayList<Dimension> dimensions) {
		this.dimensions = dimensions;
	}

	public ConnectionProperties getConnectionProperties() {
		return connectionProperties;
	}

	public void setConnectionProperties(final ConnectionProperties connectionProperties) {
		this.connectionProperties = connectionProperties;
	}

	public String getSourceDirectory() {
		return sourceDirectory;
	}

	public void setSourceDirectory(final String sourceDirectory) {
		this.sourceDirectory = sourceDirectory;
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

	public String getErrorDirectory() {
		return errorDirectory;
	}

	public void setErrorDirectory(final String errorDirectory) {
		this.errorDirectory = errorDirectory;
	}

}

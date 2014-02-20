package com.threeglav.sh.bauk.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "config")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class BaukConfiguration {

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

	@XmlElement(required = true)
	private String databaseStringLiteral;

	@XmlElement(required = true)
	private String databaseStringEscapeLiteral;

	@XmlElementWrapper(required = true)
	@XmlElement(name = "factFeed")
	private ArrayList<FactFeed> factFeeds;

	@XmlElementWrapper(required = true)
	@XmlElement(name = "dimension")
	private ArrayList<Dimension> dimensions;

	@XmlElementWrapper(required = false)
	@XmlElement(name = "property")
	private ArrayList<BaukProperty> properties;

	// optimization, not found in configuration file
	@XmlTransient
	private Map<String, Dimension> dimensionMap;

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

	public String getDatabaseStringLiteral() {
		return databaseStringLiteral;
	}

	public void setDatabaseStringLiteral(final String databaseStringLiteral) {
		this.databaseStringLiteral = databaseStringLiteral;
	}

	public String getDatabaseStringEscapeLiteral() {
		return databaseStringEscapeLiteral;
	}

	public void setDatabaseStringEscapeLiteral(final String databaseStringEscapeLiteral) {
		this.databaseStringEscapeLiteral = databaseStringEscapeLiteral;
	}

	public ArrayList<BaukProperty> getProperties() {
		return properties;
	}

	public void setProperties(final ArrayList<BaukProperty> properties) {
		this.properties = properties;
	}

	public synchronized Map<String, Dimension> getDimensionMap() {
		if (dimensionMap == null) {
			dimensionMap = new HashMap<String, Dimension>();
			for (final Dimension d : dimensions) {
				dimensionMap.put(d.getName(), d);
			}
		}
		return dimensionMap;
	}

}

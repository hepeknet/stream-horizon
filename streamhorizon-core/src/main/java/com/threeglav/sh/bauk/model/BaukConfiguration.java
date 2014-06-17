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

	private static final String DEFAULT_DATABASE_STRING_LITERAL = "'";
	private static final String DEFAULT_DATABASE_STRING_ESCAPE_LITERAL = "''";

	@XmlElement(required = true)
	private ConnectionProperties connectionProperties;

	@XmlElement(required = false, defaultValue = DEFAULT_DATABASE_STRING_LITERAL)
	private String databaseStringLiteral = DEFAULT_DATABASE_STRING_LITERAL;

	@XmlElement(required = false, defaultValue = DEFAULT_DATABASE_STRING_ESCAPE_LITERAL)
	private String databaseStringEscapeLiteral = DEFAULT_DATABASE_STRING_ESCAPE_LITERAL;

	@XmlElementWrapper(required = true)
	@XmlElement(name = "feed")
	private ArrayList<Feed> feeds;

	@XmlElementWrapper(required = true)
	@XmlElement(name = "dimension")
	private ArrayList<Dimension> dimensions;

	@XmlElementWrapper(required = false)
	@XmlElement(name = "property")
	private ArrayList<BaukProperty> properties;

	// optimization, not found in configuration file
	@XmlTransient
	private Map<String, Dimension> dimensionMap;

	public ArrayList<Feed> getFeeds() {
		return feeds;
	}

	public void setFeeds(final ArrayList<Feed> factFeeds) {
		this.feeds = factFeeds;
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

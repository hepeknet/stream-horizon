package com.threeglav.bauk.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.threeglav.bauk.SystemConfigurationConstants;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class Dimension {

	@XmlAttribute(required = true)
	private String name;

	@XmlAttribute(required = true)
	private DimensionType type;

	@XmlElementWrapper
	@XmlElement(name = "mappedColumn")
	private ArrayList<MappedColumn> mappedColumns;

	@XmlElement
	private SqlStatements sqlStatements;

	@XmlAttribute(required = false)
	private String cacheKeyPerFeedInto;

	@XmlElement(required = false, defaultValue = "" + SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_DEFAULT)
	private Integer localCacheMaxSize;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public DimensionType getType() {
		return type;
	}

	public void setType(final DimensionType type) {
		this.type = type;
	}

	public ArrayList<MappedColumn> getMappedColumns() {
		return mappedColumns;
	}

	public void setMappedColumns(final ArrayList<MappedColumn> mappedColumns) {
		this.mappedColumns = mappedColumns;
	}

	public SqlStatements getSqlStatements() {
		return sqlStatements;
	}

	public void setSqlStatements(final SqlStatements sqlStatements) {
		this.sqlStatements = sqlStatements;
	}

	public String getCacheKeyPerFeedInto() {
		return cacheKeyPerFeedInto;
	}

	public void setCacheKeyPerFeedInto(final String cacheKeyPerFeedInto) {
		this.cacheKeyPerFeedInto = cacheKeyPerFeedInto;
	}

	public Integer getLocalCacheMaxSize() {
		return localCacheMaxSize;
	}

	public void setLocalCacheMaxSize(final Integer localCacheMaxSize) {
		this.localCacheMaxSize = localCacheMaxSize;
	}

}

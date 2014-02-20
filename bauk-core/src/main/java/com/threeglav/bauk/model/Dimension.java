package com.threeglav.bauk.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.threeglav.bauk.BaukEngineConfigurationConstants;

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
	private Boolean cachePerThreadEnabled = Boolean.TRUE;

	@XmlElement(required = false, defaultValue = "" + BaukEngineConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_DEFAULT)
	private Integer localCacheMaxSize;

	@XmlAttribute(required = false)
	private Boolean exposeLastLineValueInContext = Boolean.FALSE;

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

	public Integer getLocalCacheMaxSize() {
		return localCacheMaxSize;
	}

	public void setLocalCacheMaxSize(final Integer localCacheMaxSize) {
		this.localCacheMaxSize = localCacheMaxSize;
	}

	public Boolean getExposeLastLineValueInContext() {
		return exposeLastLineValueInContext;
	}

	public void setExposeLastLineValueInContext(final Boolean exposeLastLineValueInContext) {
		this.exposeLastLineValueInContext = exposeLastLineValueInContext;
	}

	public Boolean getCachePerThreadEnabled() {
		return cachePerThreadEnabled;
	}

	public void setCachePerThreadEnabled(final Boolean cachePerThreadEnabled) {
		this.cachePerThreadEnabled = cachePerThreadEnabled;
	}

	/*
	 * Helper method - not persisted anywhere
	 */
	public int getNumberOfNaturalKeys() {
		int num = 0;
		if (this.getMappedColumns() != null && !this.getMappedColumns().isEmpty()) {
			for (final MappedColumn mp : this.getMappedColumns()) {
				if (mp.isNaturalKey()) {
					num++;
				}
			}
		}
		return num;
	}

}

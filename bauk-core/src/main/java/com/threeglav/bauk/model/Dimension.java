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

}

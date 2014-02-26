package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

@XmlRootElement()
@XmlAccessorType(XmlAccessType.FIELD)
public class MappedResultsSQLStatement {

	@XmlAttribute(required = true)
	private SqlStatementType type;

	@XmlValue
	private String sqlStatement;

	public String getSqlStatement() {
		return sqlStatement;
	}

	public void setSqlStatement(final String sqlStatement) {
		this.sqlStatement = sqlStatement;
	}

	public SqlStatementType getType() {
		return type;
	}

	public void setType(final SqlStatementType type) {
		this.type = type;
	}

}

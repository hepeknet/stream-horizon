package com.threeglav.bauk.model;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AfterBulkLoadSuccess {

	@XmlElementWrapper(name = "sqlStatements")
	@XmlElement(name = "sqlStatement")
	private List<String> sqlStatements;

	public List<String> getSqlStatements() {
		return sqlStatements;
	}

	public void setSqlStatements(final List<String> sqlStatements) {
		this.sqlStatements = sqlStatements;
	}

}

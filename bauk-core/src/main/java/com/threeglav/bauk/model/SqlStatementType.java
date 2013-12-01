package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

@XmlType
@XmlEnum
public enum SqlStatementType {

	@XmlEnumValue("select")
	SELECT,

	@XmlEnumValue("insert")
	INSERT,

	@XmlEnumValue("insert_return_key")
	INSERT_RETURN_KEY

}

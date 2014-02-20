package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

@XmlType
@XmlEnum
public enum CommandType {

	@XmlEnumValue("sql")
	SQL,

	@XmlEnumValue("shell")
	SHELL;

}

package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

@XmlType
@XmlEnum
public enum BulkLoadDefinitionOutputType {

	@XmlEnumValue("file")
	FILE,

	@XmlEnumValue("zip")
	ZIP,

	@XmlEnumValue("gz")
	GZ,

	@XmlEnumValue("jdbc")
	JDBC,

	@XmlEnumValue("none")
	NONE,

	@XmlEnumValue("pipe")
	PIPE;

}

package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

@XmlType
@XmlEnum
public enum HeaderFooterProcessType {

	@XmlEnumValue("strict")
	STRICT,

	@XmlEnumValue("normal")
	NORMAL,

	@XmlEnumValue("no_header")
	NO_HEADER,

	@XmlEnumValue("skip")
	SKIP;

}

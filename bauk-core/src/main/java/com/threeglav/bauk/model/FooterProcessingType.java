package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

@XmlType
@XmlEnum
public enum FooterProcessingType {

	@XmlEnumValue("strict")
	STRICT,

	@XmlEnumValue("no_footer")
	NO_FOOTER,

	@XmlEnumValue("skip")
	SKIP;

}

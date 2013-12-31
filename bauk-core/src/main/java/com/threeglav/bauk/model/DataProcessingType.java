package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

@XmlType
@XmlEnum
public enum DataProcessingType {

	/**
	 * Every data line will be validated - it must begin with given character
	 */
	@XmlEnumValue("normal")
	NORMAL,

	/**
	 * Skip validation
	 */
	@XmlEnumValue("no_validation")
	NO_VALIDATION;

}

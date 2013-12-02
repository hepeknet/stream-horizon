package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

@XmlType
@XmlEnum
public enum FactFeedType {

	@XmlEnumValue("full")
	FULL,

	@XmlEnumValue("repetitive")
	REPETITIVE,

	@XmlEnumValue("delta")
	DELTA,

	/**
	 * For this feed type we make row values available to post completion statements. Also, bulk output is not required
	 * for this type of feeds
	 */
	@XmlEnumValue("control")
	CONTROL;

}

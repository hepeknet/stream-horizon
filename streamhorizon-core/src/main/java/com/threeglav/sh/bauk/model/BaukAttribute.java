package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class BaukAttribute {

	@XmlAttribute(required = false)
	private String name;

	@XmlAttribute(required = false)
	private BaukAttributeType type;

	@XmlValue
	private String constantValue;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public String getConstantValue() {
		return constantValue;
	}

	public void setConstantValue(final String constantValue) {
		this.constantValue = constantValue;
	}

	public BaukAttributeType getType() {
		return type;
	}

	public void setType(final BaukAttributeType type) {
		this.type = type;
	}

}

package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Attribute {

	@XmlAttribute(required = true)
	private String name;

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

}

package com.threeglav.bauk.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class BulkLoadFileDefinition {

	@XmlElementWrapper
	@XmlElement(name = "attribute")
	private ArrayList<Attribute> attributes;

	public ArrayList<Attribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(final ArrayList<Attribute> attributes) {
		this.attributes = attributes;
	}

}

package com.threeglav.sh.bauk.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class TargetFormatDefinition {

	@XmlElementWrapper
	@XmlElement(name = "attribute")
	private ArrayList<BaukAttribute> attributes;

	public ArrayList<BaukAttribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(final ArrayList<BaukAttribute> attributes) {
		this.attributes = attributes;
	}

}

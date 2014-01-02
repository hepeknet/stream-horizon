package com.threeglav.bauk.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class Footer {

	@XmlAttribute
	private FooterProcessingType process = FooterProcessingType.STRICT;

	@XmlElement(required = false)
	private String eachLineStartsWithCharacter;

	@XmlElementWrapper
	@XmlElement(name = "attribute")
	private ArrayList<Attribute> attributes;

	public ArrayList<Attribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(final ArrayList<Attribute> attributes) {
		this.attributes = attributes;
	}

	public String getEachLineStartsWithCharacter() {
		return eachLineStartsWithCharacter;
	}

	public void setEachLineStartsWithCharacter(final String eachLineStartsWithCharacter) {
		this.eachLineStartsWithCharacter = eachLineStartsWithCharacter;
	}

	public FooterProcessingType getProcess() {
		return process;
	}

	public void setProcess(final FooterProcessingType process) {
		this.process = process;
	}

}

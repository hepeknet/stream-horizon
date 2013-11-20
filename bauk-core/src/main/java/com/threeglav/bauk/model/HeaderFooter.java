package com.threeglav.bauk.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class HeaderFooter {

	@XmlAttribute
	private HeaderFooterProcessType process = HeaderFooterProcessType.NORMAL;

	@XmlElement(required = true)
	private String eachLineStartsWithCharacter;

	@XmlElement(required = false)
	private String headerParserClassName;

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

	public HeaderFooterProcessType getProcess() {
		return process;
	}

	public void setProcess(final HeaderFooterProcessType process) {
		this.process = process;
	}

	public String getHeaderParserClassName() {
		return headerParserClassName;
	}

	public void setHeaderParserClassName(final String headerParserClassName) {
		this.headerParserClassName = headerParserClassName;
	}

}

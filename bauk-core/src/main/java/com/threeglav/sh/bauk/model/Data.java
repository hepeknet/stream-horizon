package com.threeglav.sh.bauk.model;

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
public class Data {

	@XmlAttribute
	private DataProcessingType process = DataProcessingType.NORMAL;

	@XmlElement(required = true)
	private String eachLineStartsWithCharacter;

	@XmlElement(required = false)
	private String feedDataProcessorClassName;

	@XmlElementWrapper
	@XmlElement(name = "attribute")
	private ArrayList<BaukAttribute> attributes;

	public String getEachLineStartsWithCharacter() {
		return eachLineStartsWithCharacter;
	}

	public void setEachLineStartsWithCharacter(final String eachLineStartsWithCharacter) {
		this.eachLineStartsWithCharacter = eachLineStartsWithCharacter;
	}

	public ArrayList<BaukAttribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(final ArrayList<BaukAttribute> attributes) {
		this.attributes = attributes;
	}

	public DataProcessingType getProcess() {
		return process;
	}

	public void setProcess(final DataProcessingType process) {
		this.process = process;
	}

	public String getFeedDataProcessorClassName() {
		return feedDataProcessorClassName;
	}

	public void setFeedDataProcessorClassName(final String feedDataProcessorClassName) {
		this.feedDataProcessorClassName = feedDataProcessorClassName;
	}

}

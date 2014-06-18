package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class SourceFormatDefinition {

	@XmlElement
	private String nullString;

	@XmlElement
	private String delimiterString;

	@XmlElement
	private Header header;

	@XmlElement
	private Data data;

	@XmlElement
	private Footer footer;

	public String getNullString() {
		return nullString;
	}

	public void setNullString(final String nullString) {
		this.nullString = nullString;
	}

	public String getDelimiterString() {
		return delimiterString;
	}

	public void setDelimiterString(final String delimiterString) {
		this.delimiterString = delimiterString;
	}

	public Header getHeader() {
		return header;
	}

	public void setHeader(final Header header) {
		this.header = header;
	}

	public Footer getFooter() {
		return footer;
	}

	public void setFooter(final Footer footer) {
		this.footer = footer;
	}

	public Data getData() {
		return data;
	}

	public void setData(final Data data) {
		this.data = data;
	}

}

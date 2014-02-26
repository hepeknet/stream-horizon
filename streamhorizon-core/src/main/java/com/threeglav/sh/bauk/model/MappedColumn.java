package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class MappedColumn {

	@XmlAttribute(required = true)
	private String name;

	@XmlAttribute(required = false)
	private Boolean naturalKey = Boolean.FALSE;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public Boolean isNaturalKey() {
		return naturalKey;
	}

	public void setNaturalKey(final Boolean naturalKey) {
		this.naturalKey = naturalKey;
	}

}

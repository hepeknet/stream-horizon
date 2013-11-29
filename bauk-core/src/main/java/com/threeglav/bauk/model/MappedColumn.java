package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class MappedColumn {

	@XmlAttribute(required = true)
	private String name;

	@XmlAttribute(required = false)
	private boolean naturalKey;

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public boolean isNaturalKey() {
		return naturalKey;
	}

	public void setNaturalKey(final boolean naturalKey) {
		this.naturalKey = naturalKey;
	}

}

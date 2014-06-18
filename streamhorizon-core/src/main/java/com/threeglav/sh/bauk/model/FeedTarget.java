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
public class FeedTarget {

	public static final String FILE_TARGET_EXTENSION_PROP_NAME = "bulkLoadOutputExtension";
	public static final String FILE_TARGET_DIRECTORY_PROP_NAME = "bulkOutputDirectory";

	@XmlAttribute(required = true)
	private String type = BulkLoadDefinitionOutputType.FILE.toString();

	@XmlElementWrapper(required = false)
	@XmlElement(name = "property")
	private ArrayList<BaukProperty> properties;

	public String getType() {
		return type;
	}

	public void setType(final String type) {
		this.type = type;
	}

	public ArrayList<BaukProperty> getProperties() {
		return properties;
	}

	public void setProperties(final ArrayList<BaukProperty> properties) {
		this.properties = properties;
	}

}

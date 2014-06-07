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
public class FeedSource {

	public static final String FILE_FEED_SOURCE = "file";

	public static final String FILE_FEED_SOURCE_DIRECTORY_PATH_PROPERTY_NAME = "directoryPath";

	public static final String FILE_FEED_SOURCE_FILE_NAME_MASK_PROPERTY_NAME = "fileNameMask";

	public static final String RPC_FEED_SOURCE = "rpc";

	public static final String RPC_FEED_SOURCE_SERVER_PORT_PROPERTY_NAME = "port";

	public static final String JDBC_FEED_SOURCE = "jdbc";

	public static final String JDBC_FEED_SOURCE_SQL_STATEMENT_PROPERTY_NAME = "sqlStatement";

	public static final String JDBC_FEED_SOURCE_SCHEDULE_PROPERTY_NAME = "schedule";

	public static final String JDBC_FEED_SOURCE_JDBC_URL_PROPERTY_NAME = "jdbcUrl";

	@XmlAttribute(required = true)
	private String type;

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

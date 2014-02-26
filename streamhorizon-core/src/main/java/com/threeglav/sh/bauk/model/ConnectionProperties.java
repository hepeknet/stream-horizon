package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class ConnectionProperties {

	public static final int DEFAULT_POOL_SIZE = 25;

	@XmlElement(required = true)
	private String jdbcUrl;

	@XmlElement(required = false)
	private String jdbcUserName;

	@XmlElement(required = false)
	private String jdbcPassword;

	@XmlElement(required = false, defaultValue = "25")
	private Integer jdbcPoolSize = DEFAULT_POOL_SIZE;

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(final String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getJdbcUserName() {
		return jdbcUserName;
	}

	public void setJdbcUserName(final String jdbcUserName) {
		this.jdbcUserName = jdbcUserName;
	}

	public String getJdbcPassword() {
		return jdbcPassword;
	}

	public void setJdbcPassword(final String jdbcPassword) {
		this.jdbcPassword = jdbcPassword;
	}

	public Integer getJdbcPoolSize() {
		return jdbcPoolSize;
	}

	public void setJdbcPoolSize(final Integer jdbcPoolSize) {
		this.jdbcPoolSize = jdbcPoolSize;
	}

}

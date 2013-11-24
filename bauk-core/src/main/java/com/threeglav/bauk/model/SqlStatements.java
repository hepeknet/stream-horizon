package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class SqlStatements {

	@XmlElement
	private String insertSingle;

	@XmlElement
	private String selectSurrogateKey;

	@XmlElement
	private String preCacheKeys;

	public String getInsertSingle() {
		return insertSingle;
	}

	public void setInsertSingle(final String insertSingle) {
		this.insertSingle = insertSingle;
	}

	public String getSelectSurrogateKey() {
		return selectSurrogateKey;
	}

	public void setSelectSurrogateKey(final String selectSurrogateKey) {
		this.selectSurrogateKey = selectSurrogateKey;
	}

	public String getPreCacheKeys() {
		return preCacheKeys;
	}

	public void setPreCacheKeys(final String preCacheKeys) {
		this.preCacheKeys = preCacheKeys;
	}

}

package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class SqlStatements {

	@XmlElement
	private String insertSingleRecord;

	@XmlElement
	private String selectRecordIdentifier;

	@XmlElement
	private String preCacheRecords;

	@XmlElement
	private String updateSingleRecord;

	public String getInsertSingle() {
		return insertSingleRecord;
	}

	public void setInsertSingle(final String insertSingle) {
		insertSingleRecord = insertSingle;
	}

	public String getSelectRecordIdentifier() {
		return selectRecordIdentifier;
	}

	public void setSelectRecordIdentifier(final String selectSurrogateKey) {
		selectRecordIdentifier = selectSurrogateKey;
	}

	public String getPreCacheRecords() {
		return preCacheRecords;
	}

	public void setPreCacheRecords(final String preCacheKeys) {
		preCacheRecords = preCacheKeys;
	}

	public String getUpdateSingleRecord() {
		return updateSingleRecord;
	}

	public void setUpdateSingleRecord(final String updateSingleRecord) {
		this.updateSingleRecord = updateSingleRecord;
	}

}

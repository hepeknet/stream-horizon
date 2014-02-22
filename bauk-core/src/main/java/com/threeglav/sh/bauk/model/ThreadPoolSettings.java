package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class ThreadPoolSettings {

	public static final int THREAD_POOL_DEFAULT_SIZE = 5;

	@XmlElement(required = false, defaultValue = "" + THREAD_POOL_DEFAULT_SIZE)
	private int etlProcessingThreadCount = THREAD_POOL_DEFAULT_SIZE;

	@XmlElement(required = false, defaultValue = "" + THREAD_POOL_DEFAULT_SIZE)
	private int databaseProcessingThreadCount = THREAD_POOL_DEFAULT_SIZE;

	public int getEtlProcessingThreadCount() {
		return etlProcessingThreadCount;
	}

	public void setEtlProcessingThreadCount(final int feedProcessingThreads) {
		etlProcessingThreadCount = feedProcessingThreads;
	}

	public int getDatabaseProcessingThreadCount() {
		return databaseProcessingThreadCount;
	}

	public void setDatabaseProcessingThreadCount(final int bulkLoadProcessingThreads) {
		databaseProcessingThreadCount = bulkLoadProcessingThreads;
	}

}

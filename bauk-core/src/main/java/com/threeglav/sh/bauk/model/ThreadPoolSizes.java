package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class ThreadPoolSizes {

	public static final int THREAD_POOL_DEFAULT_SIZE = 1;

	@XmlElement(required = false, defaultValue = "1")
	private int feedProcessingThreads = THREAD_POOL_DEFAULT_SIZE;

	@XmlElement(required = false, defaultValue = "1")
	private int bulkLoadProcessingThreads = THREAD_POOL_DEFAULT_SIZE;

	public int getFeedProcessingThreads() {
		return feedProcessingThreads;
	}

	public void setFeedProcessingThreads(final int feedProcessingThreads) {
		this.feedProcessingThreads = feedProcessingThreads;
	}

	public int getBulkLoadProcessingThreads() {
		return bulkLoadProcessingThreads;
	}

	public void setBulkLoadProcessingThreads(final int bulkLoadProcessingThreads) {
		this.bulkLoadProcessingThreads = bulkLoadProcessingThreads;
	}

}

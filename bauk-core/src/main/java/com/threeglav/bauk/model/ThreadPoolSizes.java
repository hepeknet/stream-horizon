package com.threeglav.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ThreadPoolSizes {

	public static final int THREAD_POOL_DEFAULT_SIZE = 1;

	@XmlElement(required = true)
	private int feedProcessingThreads = THREAD_POOL_DEFAULT_SIZE;

	@XmlElement(required = true)
	private int bulkLoadProcessingThreads = THREAD_POOL_DEFAULT_SIZE;

	public int getFeedProcessingThreads() {
		return this.feedProcessingThreads;
	}

	public void setFeedProcessingThreads(final int feedProcessingThreads) {
		this.feedProcessingThreads = feedProcessingThreads;
	}

	public int getBulkLoadProcessingThreads() {
		return this.bulkLoadProcessingThreads;
	}

	public void setBulkLoadProcessingThreads(final int bulkLoadProcessingThreads) {
		this.bulkLoadProcessingThreads = bulkLoadProcessingThreads;
	}

}

package com.threeglav.sh.bauk.model;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class FeedEvents {

	@XmlElementWrapper(name = "beforeFeedProcessing")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> beforeFeedProcessing;

	@XmlElementWrapper(name = "onStartupCommands")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onStartup;

	@XmlElementWrapper(name = "afterFeedProcessingCompletion")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> afterFeedProcessingCompletion;

	@XmlElementWrapper(name = "onFeedProcessingFailure")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onFeedProcessingFailure;

	public ArrayList<BaukCommand> getOnStartup() {
		return onStartup;
	}

	public void setOnStartup(final ArrayList<BaukCommand> onStartup) {
		this.onStartup = onStartup;
	}

	public ArrayList<BaukCommand> getAfterFeedProcessingCompletion() {
		return afterFeedProcessingCompletion;
	}

	public void setAfterFeedProcessingCompletion(final ArrayList<BaukCommand> afterFeedProcessingCompletion) {
		this.afterFeedProcessingCompletion = afterFeedProcessingCompletion;
	}

	public ArrayList<BaukCommand> getOnFeedProcessingFailure() {
		return onFeedProcessingFailure;
	}

	public void setOnFeedProcessingFailure(final ArrayList<BaukCommand> onFeedProcessingFailure) {
		this.onFeedProcessingFailure = onFeedProcessingFailure;
	}

	public ArrayList<BaukCommand> getBeforeFeedProcessing() {
		return beforeFeedProcessing;
	}

	public void setBeforeFeedProcessing(final ArrayList<BaukCommand> beforeFeedProcessing) {
		this.beforeFeedProcessing = beforeFeedProcessing;
	}

}

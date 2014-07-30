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

	@XmlElementWrapper(name = "beforeBulkLoadProcessing")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> beforeBulkLoadProcessing;

	@XmlElementWrapper(name = "onStartupCommands")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onStartup;

	@XmlElementWrapper(name = "afterFeedProcessingCompletion")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> afterFeedProcessingCompletion;

	@XmlElementWrapper(name = "afterFeedProcessingFailure")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onFeedProcessingFailure;

	@XmlElementWrapper(name = "afterFeedSuccess")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> afterFeedSuccess;

	@XmlElementWrapper(name = "afterBulkLoadSuccess")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> afterBulkLoadSuccess;

	@XmlElementWrapper(name = "afterBulkLoadFailure")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onBulkLoadFailure;

	@XmlElementWrapper(name = "afterBulkLoadCompletion")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onBulkLoadCompletion;

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

	public ArrayList<BaukCommand> getAfterBulkLoadSuccess() {
		return afterBulkLoadSuccess;
	}

	public void setAfterBulkLoadSuccess(final ArrayList<BaukCommand> afterBulkLoadSuccess) {
		this.afterBulkLoadSuccess = afterBulkLoadSuccess;
	}

	public ArrayList<BaukCommand> getOnBulkLoadFailure() {
		return onBulkLoadFailure;
	}

	public void setOnBulkLoadFailure(final ArrayList<BaukCommand> onBulkLoadFailure) {
		this.onBulkLoadFailure = onBulkLoadFailure;
	}

	public ArrayList<BaukCommand> getOnBulkLoadCompletion() {
		return onBulkLoadCompletion;
	}

	public void setOnBulkLoadCompletion(final ArrayList<BaukCommand> onBulkLoadCompletion) {
		this.onBulkLoadCompletion = onBulkLoadCompletion;
	}

	public ArrayList<BaukCommand> getBeforeBulkLoadProcessing() {
		return beforeBulkLoadProcessing;
	}

	public void setBeforeBulkLoadProcessing(final ArrayList<BaukCommand> beforeBulkLoadProcessing) {
		this.beforeBulkLoadProcessing = beforeBulkLoadProcessing;
	}

	public ArrayList<BaukCommand> getAfterFeedSuccess() {
		return afterFeedSuccess;
	}

	public void setAfterFeedSuccess(final ArrayList<BaukCommand> afterFeedSuccess) {
		this.afterFeedSuccess = afterFeedSuccess;
	}

}

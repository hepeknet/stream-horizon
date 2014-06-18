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
public class BulkLoadDefinition {

	@XmlElement(required = false)
	private TargetFormatDefinition targetFormatDefinition;

	@XmlElementWrapper(name = "afterBulkLoadSuccess")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> afterBulkLoadSuccess;

	@XmlElementWrapper(name = "onBulkLoadFailure")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onBulkLoadFailure;

	@XmlElementWrapper(name = "onBulkLoadCompletion")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> onBulkLoadCompletion;

	@XmlElementWrapper(name = "bulkLoadInsert")
	@XmlElement(name = "command")
	private ArrayList<BaukCommand> bulkLoadInsert;

	public TargetFormatDefinition getTargetFormatDefinition() {
		return targetFormatDefinition;
	}

	public void setTargetFormatDefinition(final TargetFormatDefinition bulkLoadFormatDefinition) {
		targetFormatDefinition = bulkLoadFormatDefinition;
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

	public ArrayList<BaukCommand> getBulkLoadInsert() {
		return bulkLoadInsert;
	}

	public void setBulkLoadInsert(final ArrayList<BaukCommand> bulkLoadInsert) {
		this.bulkLoadInsert = bulkLoadInsert;
	}

	public ArrayList<BaukCommand> getOnBulkLoadCompletion() {
		return onBulkLoadCompletion;
	}

	public void setOnBulkLoadCompletion(final ArrayList<BaukCommand> onBulkLoadCompletion) {
		this.onBulkLoadCompletion = onBulkLoadCompletion;
	}

}

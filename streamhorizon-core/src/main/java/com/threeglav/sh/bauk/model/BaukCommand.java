package com.threeglav.sh.bauk.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
public class BaukCommand {

	@XmlAttribute(required = true)
	private CommandType type;

	@XmlValue
	private String command;

	public CommandType getType() {
		return type;
	}

	public void setType(final CommandType type) {
		this.type = type;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(final String command) {
		this.command = command;
	}

	@Override
	public String toString() {
		return "BaukCommand [type=" + type + ", command=" + command + "]";
	}

}

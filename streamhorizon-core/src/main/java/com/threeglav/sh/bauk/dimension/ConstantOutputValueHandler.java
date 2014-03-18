package com.threeglav.sh.bauk.dimension;

import java.util.Map;

import com.threeglav.sh.bauk.BulkLoadOutputValueHandler;

public final class ConstantOutputValueHandler implements BulkLoadOutputValueHandler {

	private final String constantValue;

	public ConstantOutputValueHandler(final String constantValue) {
		this.constantValue = constantValue;
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues) {
		return constantValue;
	}

	@Override
	public void closeCurrentFeed() {

	}

	@Override
	public String getLastLineBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues) {
		return constantValue;
	}

	@Override
	public boolean closeShouldBeInvoked() {
		return false;
	}

}

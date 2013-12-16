package com.threeglav.bauk.feed.bulk.writer;

import java.util.Map;

public final class NullBulkOutputWriter implements BulkOutputWriter {

	@Override
	public void initialize(final String outputFilePath) {
	}

	@Override
	public void doOutput(final String[] resolvedData) {
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes) {
	}

}

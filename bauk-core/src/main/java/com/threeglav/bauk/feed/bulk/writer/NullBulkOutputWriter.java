package com.threeglav.bauk.feed.bulk.writer;

public final class NullBulkOutputWriter implements BulkOutputWriter {

	@Override
	public void initialize(final String outputFilePath) {
	}

	@Override
	public void doOutput(final String line) {
	}

	@Override
	public void closeResources() {
	}

}

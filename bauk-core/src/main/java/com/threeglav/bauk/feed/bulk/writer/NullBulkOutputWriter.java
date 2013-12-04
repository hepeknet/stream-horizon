package com.threeglav.bauk.feed.bulk.writer;

public final class NullBulkOutputWriter implements BulkOutputWriter {

	@Override
	public void startWriting(final String outputFilePath) {
	}

	@Override
	public void write(final String line) {
	}

	@Override
	public void closeResources() {
	}

}

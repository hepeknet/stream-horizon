package com.threeglav.bauk.feed.bulk.writer;

import java.util.Map;

public final class NullBulkOutputWriter implements BulkOutputWriter {

	@Override
	public void doOutput(final Object[] resolvedData) {
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes) {
	}

	@Override
	public void initialize(final Map<String, String> globalAttributes) {
	}

}

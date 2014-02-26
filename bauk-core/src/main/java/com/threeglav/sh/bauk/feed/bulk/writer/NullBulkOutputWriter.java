package com.threeglav.sh.bauk.feed.bulk.writer;

import java.util.Map;

public final class NullBulkOutputWriter implements BulkOutputWriter {

	@Override
	public void doOutput(final Object[] resolvedData, final Map<String, String> globalAttributes) {
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes, final boolean success) {
	}

	@Override
	public void initialize(final Map<String, String> globalAttributes) {
	}

}

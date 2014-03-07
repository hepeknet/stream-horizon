package com.threeglav.sh.bauk.feed.bulk.writer;

import java.util.Map;

import com.threeglav.sh.bauk.io.BulkOutputWriter;

public final class NullBulkOutputWriter implements BulkOutputWriter {

	@Override
	public void doWriteOutput(final Object[] resolvedData, final Map<String, String> globalAttributes) {
	}

	@Override
	public void closeResourcesAfterWriting(final Map<String, String> globalAttributes, final boolean success) {
	}

	@Override
	public void startWriting(final Map<String, String> globalAttributes) {
	}

	@Override
	public boolean understandsProtocol(final String protocol) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public void init(final Map<String, String> engineProperties) {
		throw new UnsupportedOperationException("Not supported");
	}

}

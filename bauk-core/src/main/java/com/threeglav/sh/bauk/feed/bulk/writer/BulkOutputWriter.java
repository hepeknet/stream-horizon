package com.threeglav.sh.bauk.feed.bulk.writer;

import java.util.Map;

public interface BulkOutputWriter {

	public abstract void initialize(Map<String, String> globalAttributes);

	public abstract void doOutput(Object[] resolvedData, Map<String, String> globalAttributes);

	public abstract void closeResources(final Map<String, String> globalAttributes, boolean success);

}
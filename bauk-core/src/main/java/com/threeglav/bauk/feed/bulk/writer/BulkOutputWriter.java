package com.threeglav.bauk.feed.bulk.writer;

import java.util.Map;

public interface BulkOutputWriter {

	public abstract void initialize(String outputFilePath);

	public abstract void doOutput(Object[] resolvedData);

	public abstract void closeResources(final Map<String, String> globalAttributes);

}
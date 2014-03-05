package com.threeglav.sh.bauk.io;

import java.util.Map;

public interface BulkOutputWriter {

	public abstract void startWriting(Map<String, String> globalAttributes);

	public abstract void doWriteOutput(Object[] resolvedData, Map<String, String> globalAttributes);

	public abstract void closeResourcesAfterWriting(final Map<String, String> globalAttributes, boolean success);

	public boolean understandsProtocol(String protocol);

}
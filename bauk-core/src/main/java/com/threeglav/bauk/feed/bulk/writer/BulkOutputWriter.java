package com.threeglav.bauk.feed.bulk.writer;

public interface BulkOutputWriter {

	public abstract void initialize(String outputFilePath);

	public abstract void doOutput(String line);

	public abstract void closeResources();

}
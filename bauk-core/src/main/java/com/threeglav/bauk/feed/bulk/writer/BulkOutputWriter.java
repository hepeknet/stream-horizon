package com.threeglav.bauk.feed.bulk.writer;

public interface BulkOutputWriter {

	public abstract void startWriting(String outputFilePath);

	public abstract void write(String line);

	public abstract void closeResources();

}
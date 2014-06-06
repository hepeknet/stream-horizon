package com.threeglav.sh.bauk.rpc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.files.BaukFile;

public class RPCBaukFile extends BaukFile {

	private final InputFeed feed;

	public RPCBaukFile(final InputFeed feed) {
		if (feed == null) {
			throw new IllegalArgumentException("Feed must not be null");
		}
		this.feed = feed;
		this.setSize(feed.getSizeBytes());
		this.setFileNameOnly(feed.getFeedName());
		this.setLastModifiedTime(feed.getLastModifiedTimestamp());
		this.setFullFilePath(feed.getFeedName());
	}

	@Override
	public void delete() throws IOException {
		feed.clear();
		feed.setData(null);
	}

	@Override
	public void closeResources() {
	}

	@Override
	public InputStream getInputStream() throws IOException {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < feed.getData().size(); i++) {
			if (i != 0) {
				sb.append(BaukConstants.NEWLINE_STRING);
			}
			final String val = feed.getData().get(i);
			sb.append(val);
		}
		final InputStream is = new ByteArrayInputStream(sb.toString().getBytes());
		return is;
	}

}

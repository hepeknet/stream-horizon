package com.threeglav.sh.bauk.files;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.util.StringUtil;

public class ListBaukFile extends BaukFile {

	private final List<List<String>> data;
	private final String delimiter;

	public ListBaukFile(final List<List<String>> data, final String delimiterString) {
		if (data == null || data.isEmpty()) {
			throw new IllegalArgumentException("Data must not be null or empty");
		}
		if (StringUtil.isEmpty(delimiterString)) {
			throw new IllegalArgumentException("Delimiter must not be null or empty");
		}
		this.data = data;
		delimiter = delimiterString;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < data.size(); i++) {
			if (i != 0) {
				sb.append(BaukConstants.NEWLINE_STRING);
			}
			final String val = this.rowToString(data.get(i));
			sb.append(val);
		}
		final InputStream is = new ByteArrayInputStream(sb.toString().getBytes());
		return is;
	}

	private String rowToString(final List<String> rowData) {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < rowData.size(); i++) {
			if (i != 0) {
				sb.append(delimiter);
			}
			final String val = rowData.get(i);
			sb.append(val);
		}
		return sb.toString();
	}

	@Override
	public void delete() throws IOException {
	}

}

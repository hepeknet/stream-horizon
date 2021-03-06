package com.threeglav.sh.bauk.feed;

public final class LineBuffer {

	private String line1;
	private String line2;
	private String olderLine;

	public final void add(final String line) {
		if (line1 == null) {
			line1 = line;
			olderLine = line2;
		} else if (line2 == null) {
			line2 = line;
			olderLine = line1;
		} else {
			throw new IllegalStateException("Full");
		}
	}

	public final String getLine() {
		if (line1 != null && line2 != null) {
			if (olderLine == line1) {
				olderLine = line2;
				final String res = line1;
				line1 = null;
				return res;
			} else {
				olderLine = line1;
				final String res = line2;
				line2 = null;
				return res;
			}
		} else if (line1 != null) {
			final String res = line1;
			line1 = null;
			return res;
		} else if (line2 != null) {
			final String res = line2;
			line2 = null;
			return res;
		} else {
			return null;
		}
	}

	public final boolean canAdd() {
		return line1 == null || line2 == null;
	}

	public final int getSize() {
		if (line1 != null && line2 != null) {
			return 2;
		} else if (line1 != null || line2 != null) {
			return 1;
		}
		return 0;
	}

}

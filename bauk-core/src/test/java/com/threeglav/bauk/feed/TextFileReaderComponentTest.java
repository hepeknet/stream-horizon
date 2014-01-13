package com.threeglav.bauk.feed;

import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.threeglav.bauk.feed.processing.FeedDataProcessor;
import com.threeglav.bauk.model.Attribute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.Data;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.FactFeedType;
import com.threeglav.bauk.model.Footer;
import com.threeglav.bauk.model.FooterProcessingType;
import com.threeglav.bauk.model.Header;
import com.threeglav.bauk.model.HeaderProcessingType;

public class TextFileReaderComponentTest {

	@Test
	public void testNoHeaderStrictFooter() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		final TextFileReaderComponent tfrc = new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.NO_HEADER,
				FooterProcessingType.STRICT, false), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(5, true, 0);
		final int lineNum = tfrc.process(is, null);
		Assert.assertEquals(5, tfdp.lines.size());
		Assert.assertEquals(1, tfdp.lastLineInvocations);
		Assert.assertEquals(5, tfdp.lastLineNumber);
		Assert.assertEquals("4,4,4,4,4", tfdp.lastLineContent);
		Assert.assertEquals(5, lineNum);
	}

	@Test
	public void testNoHeaderStrictFooterEven() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		final TextFileReaderComponent tfrc = new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.NO_HEADER,
				FooterProcessingType.STRICT, false), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(6, true, 0);
		final int lineNum = tfrc.process(is, null);
		Assert.assertEquals(6, tfdp.lines.size());
		Assert.assertEquals(1, tfdp.lastLineInvocations);
		Assert.assertEquals(6, tfdp.lastLineNumber);
		Assert.assertEquals("5,5,5,5,5", tfdp.lastLineContent);
		Assert.assertEquals(6, lineNum);
	}

	@Test
	public void testNoHeaderNoFooter() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		final TextFileReaderComponent tfrc = new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.NO_HEADER,
				FooterProcessingType.SKIP, false), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(5, false, 0);
		final int lineNum = tfrc.process(is, null);
		Assert.assertEquals(5, tfdp.lines.size());
		Assert.assertEquals(5, tfdp.lastLineNumber);
		Assert.assertEquals(1, tfdp.lastLineInvocations);
		Assert.assertEquals("4,4,4,4,4", tfdp.lastLineContent);
		Assert.assertEquals(5, lineNum);
	}

	@Test
	public void testSkipHeaderNoFooter() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		final TextFileReaderComponent tfrc = new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.SKIP, FooterProcessingType.SKIP,
				false), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(5, false, 0);
		final int lineNum = tfrc.process(is, null);
		Assert.assertEquals(4, tfdp.lines.size());
		Assert.assertEquals(4, tfdp.lastLineNumber);
		Assert.assertEquals(1, tfdp.lastLineInvocations);
		Assert.assertEquals("4,4,4,4,4", tfdp.lastLineContent);
		Assert.assertEquals(4, lineNum);
	}

	@Test
	public void testSkipHeaderStrictFooter() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		final TextFileReaderComponent tfrc = new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.SKIP, FooterProcessingType.STRICT,
				false), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(4, true, 1);
		final int lineNum = tfrc.process(is, null);
		Assert.assertEquals(3, tfdp.lines.size());
		Assert.assertEquals(3, tfdp.lastLineNumber);
		Assert.assertEquals(1, tfdp.lastLineInvocations);
		Assert.assertEquals("3,3,3,3,3", tfdp.lastLineContent);
		Assert.assertEquals(3, lineNum);
	}

	@Test(expected = IllegalStateException.class)
	public void testInvalidControlFeed() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.SKIP, FooterProcessingType.STRICT, true), this.createConfig(), tfdp,
				"route1");
	}

	@Test
	public void testControlFeed() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		final TextFileReaderComponent tfrc = new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.NO_HEADER,
				FooterProcessingType.SKIP, true), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(1, false, 0);
		final Map<String, String> attrs = new HashMap<String, String>();
		Assert.assertEquals(0, attrs.size());
		tfrc.process(is, attrs);
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertEquals(0, tfdp.lastLineInvocations);
		Assert.assertNull(tfdp.lastLineContent);
		Assert.assertEquals(5, attrs.size());
	}

	private InputStream createOrderedContent(final int numOfLines, final boolean addFooter, final int footerSubtract) {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < numOfLines; i++) {
			String line = "";
			for (int k = 0; k < 5; k++) {
				if (k != 0) {
					line += ",";
				}
				line += i;
			}
			sb.append(line).append("\n");
		}
		if (addFooter) {
			sb.append("9,").append(numOfLines - footerSubtract);
		}
		return new ByteArrayInputStream(sb.toString().getBytes());
	}

	private BaukConfiguration createConfig() {
		final BaukConfiguration bc = Mockito.mock(BaukConfiguration.class);
		return bc;
	}

	private FactFeed createFactFeed(final HeaderProcessingType hType, final FooterProcessingType fType, final boolean isControl) {
		final FactFeed ff = Mockito.mock(FactFeed.class);
		when(ff.getDelimiterString()).thenReturn(",");
		final Header h = Mockito.mock(Header.class);
		when(h.getProcess()).thenReturn(hType);
		when(h.getEachLineStartsWithCharacter()).thenReturn("0");
		when(ff.getHeader()).thenReturn(h);
		when(ff.getName()).thenReturn("testFeed1");

		final Footer header = Mockito.mock(Footer.class);
		when(header.getProcess()).thenReturn(fType);
		when(ff.getFooter()).thenReturn(header);
		when(header.getEachLineStartsWithCharacter()).thenReturn("9");
		if (isControl) {
			when(ff.getType()).thenReturn(FactFeedType.CONTROL);
		} else {
			when(ff.getType()).thenReturn(FactFeedType.FULL);
		}
		final Data data = Mockito.mock(Data.class);
		when(ff.getData()).thenReturn(data);
		final ArrayList<Attribute> attrs = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			final Attribute attr = new Attribute();
			attr.setName("a_" + i);
			attrs.add(attr);
		}
		when(data.getAttributes()).thenReturn(attrs);
		return ff;
	}

	static class TestFeedDataProcessor implements FeedDataProcessor {

		private final List<String> lines = new LinkedList<>();
		private int lastLineNumber;
		private int lineCounter = 1;
		private String lastLineContent;
		private int lastLineInvocations = 0;

		@Override
		public void startFeed(final Map<String, String> globalAttributes) {
			lineCounter = 1;
		}

		@Override
		public void processLine(final String line, final Map<String, String> globalAttributes) {
			lines.add(line);
			lineCounter++;
		}

		@Override
		public void closeFeed(final int expectedResults, final Map<String, String> globalAttributes) {

		}

		public void clear() {
			lines.clear();
			lastLineNumber = 0;
			lineCounter = 1;
			lastLineContent = null;
			lastLineInvocations = 0;
		}

		@Override
		public void processLastLine(final String line, final Map<String, String> globalAttributes) {
			lines.add(line);
			lastLineNumber = lineCounter;
			lineCounter++;
			lastLineContent = line;
			lastLineInvocations++;
		}

	}

}

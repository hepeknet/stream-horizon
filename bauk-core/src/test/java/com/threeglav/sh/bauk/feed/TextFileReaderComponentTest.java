package com.threeglav.sh.bauk.feed;

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

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.feed.TextFileReaderComponent;
import com.threeglav.sh.bauk.feed.processing.FeedDataProcessor;
import com.threeglav.sh.bauk.model.BaukAttribute;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Data;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.model.FactFeedType;
import com.threeglav.sh.bauk.model.Footer;
import com.threeglav.sh.bauk.model.FooterProcessingType;
import com.threeglav.sh.bauk.model.Header;
import com.threeglav.sh.bauk.model.HeaderProcessingType;

public class TextFileReaderComponentTest {

	@Test
	public void testNoHeaderStrictFooter() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		final TextFileReaderComponent tfrc = new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.NO_HEADER,
				FooterProcessingType.STRICT, false, 1), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(5, true, 0, 0);
		final Map<String, String> globalAttrs = new HashMap<String, String>();
		final int lineNum = tfrc.process(is, globalAttrs);
		Assert.assertEquals(5, tfdp.lines.size());
		Assert.assertEquals(1, tfdp.lastLineInvocations);
		Assert.assertEquals(5, tfdp.lastLineNumber);
		Assert.assertEquals("4,4,4,4,4", tfdp.lastLineContent);
		Assert.assertEquals(5, lineNum);
		Assert.assertEquals(1, globalAttrs.size());
		Assert.assertNotNull(globalAttrs.get(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_FINISHED_TIMESTAMP));
	}

	@Test
	public void testNoHeaderStrictFooterAdditionalFooterAttributes() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		final TextFileReaderComponent tfrc = new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.NO_HEADER,
				FooterProcessingType.STRICT, false, 2), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(5, true, 0, 1);
		final Map<String, String> globalAttrs = new HashMap<String, String>();
		final int lineNum = tfrc.process(is, globalAttrs);
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
				FooterProcessingType.STRICT, false, 1), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(6, true, 0, 0);
		final Map<String, String> globalAttrs = new HashMap<String, String>();
		final int lineNum = tfrc.process(is, globalAttrs);
		Assert.assertEquals(6, tfdp.lines.size());
		Assert.assertEquals(1, tfdp.lastLineInvocations);
		Assert.assertEquals(6, tfdp.lastLineNumber);
		Assert.assertEquals("5,5,5,5,5", tfdp.lastLineContent);
		Assert.assertEquals(6, lineNum);
	}

	@Test
	public void testNoHeaderStrictFooterEvenAdditionalAttributes() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		final TextFileReaderComponent tfrc = new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.NO_HEADER,
				FooterProcessingType.STRICT, false, 4), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(6, true, 0, 3);
		final Map<String, String> globalAttrs = new HashMap<String, String>();
		final int lineNum = tfrc.process(is, globalAttrs);
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
				FooterProcessingType.SKIP, false, 2), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(5, false, 0, 0);
		final Map<String, String> globalAttrs = new HashMap<String, String>();
		final int lineNum = tfrc.process(is, globalAttrs);
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
				false, 2), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(5, false, 0, 0);
		final Map<String, String> globalAttrs = new HashMap<String, String>();
		final int lineNum = tfrc.process(is, globalAttrs);
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
				false, 1), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(4, true, 1, 0);
		final Map<String, String> globalAttrs = new HashMap<String, String>();
		final int lineNum = tfrc.process(is, globalAttrs);
		Assert.assertEquals(3, tfdp.lines.size());
		Assert.assertEquals(3, tfdp.lastLineNumber);
		Assert.assertEquals(1, tfdp.lastLineInvocations);
		Assert.assertEquals("3,3,3,3,3", tfdp.lastLineContent);
		Assert.assertEquals(3, lineNum);
	}

	@Test(expected = IllegalStateException.class)
	public void testInvalidControlFeed() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.SKIP, FooterProcessingType.STRICT, true, 2), this.createConfig(), tfdp,
				"route1");
	}

	@Test
	public void testControlFeed() {
		final TestFeedDataProcessor tfdp = new TestFeedDataProcessor();
		final TextFileReaderComponent tfrc = new TextFileReaderComponent(this.createFactFeed(HeaderProcessingType.NO_HEADER,
				FooterProcessingType.SKIP, true, 2), this.createConfig(), tfdp, "route1");
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertNull(tfdp.lastLineContent);
		final InputStream is = this.createOrderedContent(1, false, 0, 0);
		final Map<String, String> attrs = new HashMap<String, String>();
		Assert.assertEquals(0, attrs.size());
		tfrc.process(is, attrs);
		Assert.assertEquals(0, tfdp.lines.size());
		Assert.assertEquals(0, tfdp.lastLineNumber);
		Assert.assertEquals(0, tfdp.lastLineInvocations);
		Assert.assertNull(tfdp.lastLineContent);
		Assert.assertEquals(6, attrs.size());
	}

	private InputStream createOrderedContent(final int numOfLines, final boolean addFooter, final int footerSubtract,
			final int additionalFooterAttributes) {
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
			sb.append("9,");
			for (int i = 0; i < additionalFooterAttributes; i++) {
				sb.append("att,");
			}
			sb.append(numOfLines - footerSubtract);
		}
		return new ByteArrayInputStream(sb.toString().getBytes());
	}

	private BaukConfiguration createConfig() {
		final BaukConfiguration bc = Mockito.mock(BaukConfiguration.class);
		return bc;
	}

	private FactFeed createFactFeed(final HeaderProcessingType hType, final FooterProcessingType fType, final boolean isControl,
			final int footerAttributePosition) {
		final FactFeed ff = Mockito.mock(FactFeed.class);
		when(ff.getDelimiterString()).thenReturn(",");
		final Header h = Mockito.mock(Header.class);
		when(h.getProcess()).thenReturn(hType);
		when(h.getEachLineStartsWithCharacter()).thenReturn("0");
		when(ff.getHeader()).thenReturn(h);
		when(ff.getName()).thenReturn("testFeed1");

		final Footer footer = Mockito.mock(Footer.class);
		when(footer.getProcess()).thenReturn(fType);
		when(footer.getRecordCountAttributePosition()).thenReturn(footerAttributePosition);
		when(ff.getFooter()).thenReturn(footer);
		when(footer.getEachLineStartsWithCharacter()).thenReturn("9");
		if (isControl) {
			when(ff.getType()).thenReturn(FactFeedType.CONTROL);
		} else {
			when(ff.getType()).thenReturn(FactFeedType.FULL);
		}
		final Data data = Mockito.mock(Data.class);
		when(ff.getData()).thenReturn(data);
		final ArrayList<BaukAttribute> attrs = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			final BaukAttribute attr = new BaukAttribute();
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
		public void closeFeed(final int expectedResults, final Map<String, String> globalAttributes, final boolean success) {

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

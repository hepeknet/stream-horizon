package com.threeglav.bauk.feed.processing;

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.threeglav.bauk.model.Attribute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.BulkLoadDefinition;
import com.threeglav.bauk.model.BulkLoadDefinitionOutputType;
import com.threeglav.bauk.model.BulkLoadFormatDefinition;
import com.threeglav.bauk.model.Data;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.FactFeedType;

@Ignore
public class MultiThreadedFeedDataProcessorTest {

	@Test
	public void testSimple() {
		final MultiThreadedFeedDataProcessor mtfdp = new MultiThreadedFeedDataProcessor(this.createFeed(), this.createConfig(), "routeId", 10);
		final Map<String, String> attrs = new HashMap<String, String>();
		mtfdp.startFeed(attrs);
		final int totalLines = 100;
		for (int i = 0; i < totalLines; i++) {
			mtfdp.processLine("abc,def,abc", attrs, false);
		}
		mtfdp.closeFeed(100, attrs);
	}

	@Test
	public void testMultipleThreadsInvocation() throws Exception {
		final MultiThreadedFeedDataProcessor mtfdp = new MultiThreadedFeedDataProcessor(this.createFeed(), this.createConfig(), "routeId", 10);
		final Map<String, String> attrs = new HashMap<String, String>();
		mtfdp.startFeed(attrs);
		final int totalLines = 100;
		final ExecutorService exec = Executors.newFixedThreadPool(10);
		final Future<Integer> f = exec.submit(new Callable<Integer>() {
			@Override
			public Integer call() {
				for (int i = 0; i < totalLines; i++) {
					mtfdp.processLine("abc,def,abc", attrs, false);
					if (i % 10 == 0) {
						try {
							Thread.sleep(10);
						} catch (final InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				return totalLines;
			}
		});
		Assert.assertEquals(Integer.valueOf(totalLines), f.get());
		Thread.sleep(1000);
		mtfdp.closeFeed(totalLines, attrs);
	}

	@Test
	public void testInvertedInvocation() throws InterruptedException, ExecutionException {
		final MultiThreadedFeedDataProcessor mtfdp = new MultiThreadedFeedDataProcessor(this.createFeed(), this.createConfig(), "routeId", 10);
		final Map<String, String> attrs = new HashMap<String, String>();
		mtfdp.startFeed(attrs);
		final int totalLines = 10000;
		final ExecutorService exec = Executors.newFixedThreadPool(10);
		final Future<Boolean> f1 = exec.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() {
				mtfdp.closeFeed(totalLines, attrs);
				return true;
			}
		});
		final Future<Integer> f2 = exec.submit(new Callable<Integer>() {
			@Override
			public Integer call() {
				try {
					Thread.sleep(100);
				} catch (final InterruptedException e1) {
					e1.printStackTrace();
				}
				for (int i = 0; i < totalLines; i++) {
					mtfdp.processLine("abc,def,abc", attrs, false);
					if (i % 1000 == 0) {
						try {
							int rand = new Random().nextInt(200);
							rand = rand + 10;
							Thread.sleep(rand);
						} catch (final InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				return totalLines;
			}
		});
		Assert.assertEquals(Integer.valueOf(totalLines), f2.get());
		Assert.assertTrue(f1.get());
	}

	@Test
	public void testInvertedInvocationManyThreads() throws Exception {
		final MultiThreadedFeedDataProcessor mtfdp = new MultiThreadedFeedDataProcessor(this.createFeed(), this.createConfig(), "routeId", 10);
		final Map<String, String> attrs = new HashMap<String, String>();
		mtfdp.startFeed(attrs);
		final int totalLines = 10000;
		final ExecutorService exec = Executors.newFixedThreadPool(10);
		final Future<Boolean> f1 = exec.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() {
				mtfdp.closeFeed(totalLines, attrs);
				return true;
			}
		});
		final List<Future<Integer>> futures = new ArrayList<>();
		final int threadNum = 4;
		for (int i = 0; i < threadNum; i++) {
			final Future<Integer> f = exec.submit(new Callable<Integer>() {
				@Override
				public Integer call() {
					try {
						Thread.sleep(10);
					} catch (final InterruptedException e1) {
						e1.printStackTrace();
					}
					for (int i = 0; i < totalLines / threadNum; i++) {
						mtfdp.processLine("abc,def,abc", attrs, false);
						if (i % 1000 == 0) {
							try {
								int rand = new Random().nextInt(200);
								rand = rand + 10;
								Thread.sleep(rand);
							} catch (final InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
					return totalLines / threadNum;
				}
			});
			futures.add(f);
		}
		for (final Future<Integer> f : futures) {
			Assert.assertEquals(Integer.valueOf(totalLines / threadNum), f.get());
		}
		Assert.assertTrue(f1.get());
	}

	private FactFeed createFeed() {
		final FactFeed ff = Mockito.mock(FactFeed.class);
		when(ff.getName()).thenReturn("testFF1");
		final BulkLoadDefinition bld = Mockito.mock(BulkLoadDefinition.class);
		when(bld.getOutputType()).thenReturn(BulkLoadDefinitionOutputType.NONE);
		final BulkLoadFormatDefinition blfd = Mockito.mock(BulkLoadFormatDefinition.class);
		final ArrayList<Attribute> attrs = new ArrayList<>();
		final Attribute at1 = new Attribute();
		attrs.add(at1);
		when(blfd.getAttributes()).thenReturn(attrs);
		when(bld.getBulkLoadFormatDefinition()).thenReturn(blfd);
		when(ff.getBulkLoadDefinition()).thenReturn(bld);

		final Data feedData = Mockito.mock(Data.class);
		when(feedData.getAttributes()).thenReturn(attrs);
		when(ff.getData()).thenReturn(feedData);
		when(ff.getDelimiterString()).thenReturn(",");
		when(ff.getType()).thenReturn(FactFeedType.FULL);
		return ff;
	}

	private BaukConfiguration createConfig() {
		final BaukConfiguration bc = Mockito.mock(BaukConfiguration.class);
		final ArrayList<Dimension> dims = new ArrayList<>();
		dims.add(this.createDimension("dim1"));
		when(bc.getDimensions()).thenReturn(dims);
		return bc;
	}

	private Dimension createDimension(final String name) {
		final Dimension dim = Mockito.mock(Dimension.class);
		when(dim.getName()).thenReturn(name);
		return dim;
	}

}

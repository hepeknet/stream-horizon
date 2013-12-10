package com.threeglav.bauk.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class BaukThreadFactory implements ThreadFactory {

	private final AtomicInteger counter = new AtomicInteger(0);
	private final String threadGroup;
	private final String threadNamePrefix;

	public BaukThreadFactory(final String threadGroup, final String threadNamePrefix) {
		this.threadGroup = threadGroup;
		this.threadNamePrefix = threadNamePrefix;
	}

	@Override
	public Thread newThread(final Runnable r) {
		final ThreadGroup tg = new ThreadGroup(threadGroup);
		final Thread t = new Thread(tg, r, threadNamePrefix + "-" + counter.incrementAndGet());
		return t;
	}

}

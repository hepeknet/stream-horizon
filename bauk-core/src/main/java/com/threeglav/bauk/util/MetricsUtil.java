package com.threeglav.bauk.util;

import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.BaukEngineConfigurationConstants;

public abstract class MetricsUtil {

	private static final boolean metricsOff;

	private static MetricRegistry registry;

	static {
		metricsOff = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.METRICS_OFF_SYS_PARAM_NAME, false);
		if (!metricsOff) {
			registry = new MetricRegistry();
			final JmxReporter reporter = JmxReporter.forRegistry(registry).inDomain("bauk-metrics").build();
			reporter.start();
		}
	}

	public static final boolean isMetricsOff() {
		return metricsOff;
	}

	public static synchronized Meter createMeter(final String name) {
		if (!metricsOff) {
			final SortedMap<String, Meter> meters = registry.getMeters();
			final Meter existingMeter = meters.get(name);
			if (existingMeter != null) {
				return existingMeter;
			}
			return registry.meter(name);
		}
		return null;
	}

	public static synchronized Counter createCounter(final String name) {
		if (!metricsOff) {
			final SortedMap<String, Counter> counters = registry.getCounters();
			final Counter existingCounter = counters.get(name);
			if (existingCounter != null) {
				return existingCounter;
			}
			return registry.counter(name);
		}
		return null;
	}

	public static synchronized Histogram createHistogram(final String name) {
		if (!metricsOff) {
			final SortedMap<String, Histogram> histograms = registry.getHistograms();
			final Histogram existingHistogram = histograms.get(name);
			if (existingHistogram != null) {
				return existingHistogram;
			}
			return registry.histogram(name);
		}
		return null;
	}

}

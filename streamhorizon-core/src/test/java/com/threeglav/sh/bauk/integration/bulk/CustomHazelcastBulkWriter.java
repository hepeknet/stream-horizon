package com.threeglav.sh.bauk.integration.bulk;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.IMap;
import com.threeglav.sh.bauk.io.BulkOutputWriter;

public class CustomHazelcastBulkWriter implements BulkOutputWriter {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private IMap<Integer, String> values;
	private int counter;
	private final IMap<String, String> attributes = CustomBulkWriterTest.INSTANCE.getMap("context_attributes");

	@Override
	public void startWriting(final Map<String, String> globalAttributes) {
		values = CustomBulkWriterTest.INSTANCE.getMap("custom_bulk");
		counter = 0;
	}

	@Override
	public void doWriteOutput(final Object[] resolvedData, final Map<String, String> globalAttributes) {
		String allVals = "";
		for (final Object o : resolvedData) {
			allVals += String.valueOf(o) + "#";
		}
		counter++;
		values.put(counter, allVals);
		log.info("Wrote data {} to hazelcast", allVals);
		attributes.putAll(globalAttributes);
		log.info("Put all context attributes {}", globalAttributes);
	}

	@Override
	public void closeResourcesAfterWriting(final Map<String, String> globalAttributes, final boolean success) {
	}

	@Override
	public boolean understandsProtocol(final String protocol) {
		return protocol != null && protocol.toLowerCase().equals("hazelcast");
	}

}

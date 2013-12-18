package com.threeglav.bauk.chain.components;

import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import com.threeglav.bauk.model.Attribute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.Data;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.DimensionType;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.FactFeedType;
import com.threeglav.bauk.model.HeaderFooter;
import com.threeglav.bauk.model.SqlStatements;

public class TestJaxb {

	public static void main(final String[] args) throws Exception {
		final BaukConfiguration c = new BaukConfiguration();
		c.setSourceDirectory("c:/a/b/c/");
		c.setArchiveDirectory("d:/a/b/c/");
		final ArrayList<Dimension> dims = new ArrayList<Dimension>();
		final Dimension d1 = new Dimension();
		final SqlStatements ss = new SqlStatements();
		ss.setInsertSingle("select 1");
		ss.setSelectSurrogateKey("select ss, ab, cc");
		d1.setSqlStatements(ss);
		d1.setName("dim1");

		d1.setType(DimensionType.T1_INSERT_ONLY);
		dims.add(d1);

		final Dimension d2 = new Dimension();
		final SqlStatements ss1 = new SqlStatements();
		ss1.setInsertSingle("select 1");
		ss1.setSelectSurrogateKey("select ss, ab, cc");
		d1.setSqlStatements(ss1);
		d2.setName("dim2");

		d2.setType(DimensionType.T1);
		dims.add(d2);
		c.setDimensions(dims);
		final ArrayList<FactFeed> feeds = new ArrayList<FactFeed>();
		final FactFeed ff = new FactFeed();
		ff.setDelimiterString("@@");
		final ArrayList<String> masks = new ArrayList<>();
		masks.add("*.abc");
		masks.add("*.zip");
		ff.setFileNameMasks(masks);
		ff.setName("ff1");
		ff.setNullString(".");
		ff.setType(FactFeedType.DELTA);
		final HeaderFooter h = new HeaderFooter();
		h.setEachLineStartsWithCharacter("a");
		ff.setHeader(h);
		final ArrayList<Attribute> attrs = new ArrayList<Attribute>();
		final Attribute a1 = new Attribute();
		a1.setName("abc");
		attrs.add(a1);
		h.setAttributes(attrs);
		final HeaderFooter f = new HeaderFooter();
		f.setEachLineStartsWithCharacter("b");
		f.setAttributes(attrs);
		ff.setFooter(f);
		final Data d = new Data();
		d.setAttributes(attrs);
		d.setEachLineStartsWithCharacter("cc");
		ff.setData(d);
		feeds.add(ff);
		c.setFactFeeds(feeds);

		final JAXBContext jaxbContext = JAXBContext.newInstance(BaukConfiguration.class);
		final Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

		// output pretty printed
		jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

		jaxbMarshaller.marshal(c, System.out);
	}
}

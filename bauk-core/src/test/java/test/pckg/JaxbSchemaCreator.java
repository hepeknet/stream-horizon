package test.pckg;

import java.io.File;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import com.threeglav.bauk.model.BaukConfiguration;

public class JaxbSchemaCreator {

	public static void main(final String[] args) throws Exception {
		final JAXBContext jaxbContext = JAXBContext.newInstance(BaukConfiguration.class);
		final SchemaOutputResolver sor = new MySchemaOutputResolver();
		jaxbContext.generateSchema(sor);
	}

	static class MySchemaOutputResolver extends SchemaOutputResolver {

		@Override
		public Result createOutput(final String namespaceURI, final String suggestedFileName) throws IOException {
			final File parentFolder = new File("d:/");
			final File file = new File(parentFolder, suggestedFileName);
			final StreamResult result = new StreamResult(file);
			result.setSystemId(file.toURI().toURL().toString());
			return result;
		}

	}

}

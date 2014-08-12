import java.util.Map;
import java.util.Random;

import com.threeglav.sh.bauk.dimension.DimensionCache;
import com.threeglav.sh.bauk.dimension.SurrogateKeyProvider;

public class CustomSurrogateKeyProvider implements SurrogateKeyProvider {

	@Override
	public Object getSurrogateKeyValue(final String[] naturalKeyValues, final Map<String, String> globalAttributes,
			final DimensionCache dimensionCache) {
		final Integer surrogateKey = this.queryWebServiceForSurrogateKey(naturalKeyValues);
		return surrogateKey;
	}

	/*
	 * Assume that we are querying some web service to get mapping from natural key to surrogate key. This might also be
	 * querying of database, cache, file system or some kind of algorithm
	 */
	private Integer queryWebServiceForSurrogateKey(final String[] naturalKeyValues) {
		return new Random().nextInt();
	}

}

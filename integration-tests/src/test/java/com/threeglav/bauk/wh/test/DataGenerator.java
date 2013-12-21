package com.threeglav.bauk.wh.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

public class DataGenerator {

	private static final int ROW_PER_FILE = 100000;
	private static final String VALUE_DELIMITER = "^$";
	private static final String DATE_TIME_FORMAT = "dd/MM/yyyy hh:mm:ss";
	private static final String DATE_ONLY_FORMAT = "dd/MM/yyyy";

	public static void main(final String[] args) throws Exception {
		generateFile();
	}

	private static void generateFile() throws Exception {
		final String guid = createRandomValues("guid", 100)[0];
		final String fileName = "d:/CSRISKFEED_" + guid + ".done";
		final BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
		bw.write(createHeader(guid));
		bw.write("\n");
		final Random rand = new Random();

		final String[] factors = createRandomValues("factor", 1000);
		final String[] under = createRandomValues("underlying", 15);
		final String[] riskunderCode = createRandomValues("riskUnderlying", 15);
		final String[] gridCellType = createRandomValues("gridCellType", 15);
		final String[] buckets = createRandomValues("bucket1|bucket2|bucket", 1000);
		final String[] curveType = createRandomValues("curvetype", 1);
		final int currencyValues = 100;
		final String[] riskCurrCode = createRandomValues("riskCurrencyCode", currencyValues);
		final String[] baseCurrCode = createRandomValues("baseCurrencyCode", currencyValues);
		final String[] exceptionCode = createRandomValues("ExceptionCode", 1000);
		final String[] exceptionDescription = createRandomValues("Exception Description", 1000);
		final String[] exceptionFlag = { "0", "1" };
		final int dealValues = 100;
		final String[] components = createRandomValues("component", dealValues);
		final String[] sourceName = createRandomValues("sourceName", dealValues);
		final String[] dealType = createRandomValues("dealType", dealValues);
		final String[] dealId = createRandomValues("dealId", dealValues);
		final String[] dealWhatIf = createRandomValues("dealWhatIf", dealValues);
		final String[] pricingModel = createRandomValues("pricingModel", 5);
		final String[] localCurrencyCode = createRandomValues("localCurrencyCode", currencyValues);
		final String[] dealDate = createRandomDates(10, DATE_TIME_FORMAT);
		final String[] dealAmmended = createRandomDates(10, DATE_TIME_FORMAT);
		final String[] dealState = createRandomValues("dealState", 10);
		final String[] structureCd = createRandomValues("structureCd", 10);
		final String[] orgCode = createRandomValues("orgCode", 10);
		final String[] productCode = createRandomValues("productCode", 10);
		final String[] partyName = createRandomValues("partyName", 10);
		final String[] tradeQl = createRandomValues("tradeQl", 100);
		final String[] tradeTDS = createRandomValues("tradeTDS", 100);
		final String[] maturityDate = createRandomDates(100, DATE_TIME_FORMAT);
		for (int i = 0; i < ROW_PER_FILE; i++) {
			final StringBuilder sb = new StringBuilder();
			sb.append("1");
			final int dealVal = rand.nextInt(dealValues);
			sb.append(VALUE_DELIMITER);
			sb.append(components[dealVal]);
			sb.append(VALUE_DELIMITER);
			sb.append(factors[rand.nextInt(1000)]);
			sb.append(VALUE_DELIMITER);
			sb.append(under[rand.nextInt(15)]);
			sb.append(VALUE_DELIMITER);
			sb.append(riskunderCode[rand.nextInt(15)]);
			sb.append(VALUE_DELIMITER);
			sb.append(gridCellType[rand.nextInt(15)]);
			sb.append(VALUE_DELIMITER);
			sb.append(buckets[rand.nextInt(15)]);
			sb.append(VALUE_DELIMITER);
			final int currValue = rand.nextInt(currencyValues);
			sb.append(riskCurrCode[currValue]);
			sb.append(VALUE_DELIMITER);
			final int riskValue = rand.nextInt(10000);
			sb.append(riskValue);
			sb.append(VALUE_DELIMITER);
			sb.append(baseCurrCode[currValue]);
			sb.append(VALUE_DELIMITER);
			final double baseValue = rand.nextDouble() * 1000;
			sb.append(baseValue);
			sb.append(VALUE_DELIMITER);
			final int riskStatus = rand.nextInt(1000);
			sb.append(exceptionCode[riskStatus]);
			sb.append(VALUE_DELIMITER);
			sb.append(exceptionDescription[riskStatus]);
			sb.append(VALUE_DELIMITER);
			sb.append(exceptionFlag[rand.nextInt(2)]);
			sb.append(VALUE_DELIMITER);
			sb.append(sourceName[dealVal]);
			sb.append(VALUE_DELIMITER);
			sb.append(dealType[dealVal]);
			sb.append(VALUE_DELIMITER);
			sb.append(dealId[dealVal]);
			sb.append(VALUE_DELIMITER);
			final int dealVersion = rand.nextInt(10);
			sb.append(dealVersion);
			sb.append(VALUE_DELIMITER);
			sb.append(dealWhatIf[dealVal]);
			sb.append(VALUE_DELIMITER);
			sb.append(dealDate[rand.nextInt(10)]);
			sb.append(VALUE_DELIMITER);
			sb.append(dealAmmended[rand.nextInt(10)]);
			sb.append(VALUE_DELIMITER);
			sb.append(dealState[rand.nextInt(10)]);
			sb.append(VALUE_DELIMITER);
			sb.append(structureCd[rand.nextInt(10)]);
			sb.append(VALUE_DELIMITER);
			sb.append(orgCode[rand.nextInt(10)]);
			sb.append(VALUE_DELIMITER);
			sb.append(productCode[rand.nextInt(10)]);
			sb.append(VALUE_DELIMITER);
			sb.append(partyName[rand.nextInt(10)]);
			sb.append(VALUE_DELIMITER);
			sb.append(tradeQl[rand.nextInt(100)]);
			sb.append(VALUE_DELIMITER);
			sb.append(tradeTDS[rand.nextInt(100)]);
			sb.append(VALUE_DELIMITER);
			sb.append(maturityDate[rand.nextInt(100)]);
			sb.append(VALUE_DELIMITER);
			sb.append(localCurrencyCode[currValue]);
			sb.append(VALUE_DELIMITER);
			final double localValue = rand.nextDouble() * 2000;
			sb.append(localValue);
			sb.append(VALUE_DELIMITER);
			sb.append(pricingModel[rand.nextInt(5)]);
			sb.append(VALUE_DELIMITER);
			sb.append(curveType[rand.nextInt(1)]);
			bw.write(sb.toString());
			bw.write("\n");
		}
		final StringBuilder sb = new StringBuilder();
		sb.append("9");
		sb.append(VALUE_DELIMITER);
		sb.append(ROW_PER_FILE);
		sb.append("\n");
		bw.write(sb.toString());
		bw.close();
	}

	private static String createHeader(final String guid) {
		final StringBuilder sb = new StringBuilder();
		sb.append("0").append(VALUE_DELIMITER);
		sb.append("fileUserName1").append(VALUE_DELIMITER);
		sb.append("qlVersion1").append(VALUE_DELIMITER);
		sb.append("portfolio1").append(VALUE_DELIMITER);
		sb.append("EOD").append(VALUE_DELIMITER);
		sb.append("location1").append(VALUE_DELIMITER);
		sb.append("intraDayName1").append(VALUE_DELIMITER);
		sb.append(createRandomDates(1, DATE_ONLY_FORMAT)[0]).append(VALUE_DELIMITER);
		sb.append("rerunVersion1").append(VALUE_DELIMITER);
		sb.append("curveType1").append(VALUE_DELIMITER);
		sb.append("parisRequest1").append(VALUE_DELIMITER);
		sb.append(guid).append(VALUE_DELIMITER);
		sb.append("rerunGuid1").append(VALUE_DELIMITER);
		sb.append(System.currentTimeMillis()).append(VALUE_DELIMITER);
		sb.append("requestGuid1").append(VALUE_DELIMITER);
		sb.append("runTag1");
		return sb.toString();
	}

	private static String[] createRandomDates(final int count, final String dateFormat) {
		final String[] vals = new String[count];
		final DateFormat formatter = new SimpleDateFormat(dateFormat);
		final Random r = new Random();
		for (int i = 0; i < count; i++) {
			final Calendar cal = Calendar.getInstance();
			final int dayNum = -1 * r.nextInt(10);
			cal.add(Calendar.DAY_OF_MONTH, dayNum);
			if (r.nextBoolean()) {
				cal.add(Calendar.DAY_OF_YEAR, dayNum);
			}
			vals[i] = formatter.format(cal.getTime());
		}
		return vals;
	}

	private static String[] createAlphabetValues(final int count) {
		final String[] vals = new String[count];
		int i = 0;
		while (i < count) {
			for (char ch = 'A'; ch <= 'Z'; ch++) {
				vals[i++] = String.valueOf(ch);
				if (i == count) {
					break;
				}
			}
		}
		return vals;
	}

	private static String[] createRandomValues(final String prefix, final int count) {
		final String[] vals = new String[count];
		final Random r = new Random();
		for (int i = 0; i < count; i++) {
			final String val = prefix + "_" + r.nextInt(count * count);
			vals[i] = val;
		}
		return vals;
	}

}

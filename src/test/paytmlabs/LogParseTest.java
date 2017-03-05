package paytmlabs;

import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

public class LogParseTest {

	private final static String SAMPLE_LOG = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2";

	private final static Pattern TOKENS_DELIMITER = Pattern.compile("\"?\\s\"?");
	private final static Pattern PORT_DELIMITER = Pattern.compile(":");
	DateTimeFormatter timeFormat = ISODateTimeFormat.dateTime();

	@Test
	public void pareTest() {
		String[] logTokens = TOKENS_DELIMITER.split(SAMPLE_LOG);
		DateTime timestamp = timeFormat.parseDateTime(logTokens[0]);
		Assert.assertEquals(1437555628019L, timestamp.getMillis());
		Assert.assertEquals("123.242.248.130", PORT_DELIMITER.split(logTokens[2])[0]);
		Assert.assertEquals(
				"https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null",
				logTokens[12]);
	}
}

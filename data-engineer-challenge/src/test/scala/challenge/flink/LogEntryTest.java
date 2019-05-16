package challenge.flink;

import org.apache.flink.types.Row;
import org.junit.Assert;

import java.time.Instant;

public class LogEntryTest {
    private static final String log_sample = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2";

    @org.junit.Test
    public void parse() {
        // See: https://docs.aws.amazon.com/athena/latest/ug/elasticloadbalancer-classic-logs.html
        // timestamp string,
        // elb_name string,
        // request_ip string,
        // request_port int,
        // backend_ip string,
        // backend_port int,
        // request_processing_time double,
        // backend_processing_time double,
        // client_response_time double,
        // elb_response_code string,
        // backend_response_code string,
        // received_bytes bigint,
        // sent_bytes bigint,
        // request_verb string,
        // url string,
        // protocol string,
        // user_agent string,
        // ssl_cipher string,
        // ssl_protocol string
        Row parse = LogEntry.parse(log_sample);
        Assert.assertEquals(19, parse.getArity());
        Assert.assertEquals(java.sql.Timestamp.from(Instant.parse("2015-07-22T09:00:28.019143Z")), parse.getField(0));
        Assert.assertEquals("marketpalce-shop", parse.getField(1));
        Assert.assertEquals("123.242.248.130", parse.getField(2));
        Assert.assertEquals(54635, parse.getField(3));
        Assert.assertEquals("10.0.6.158", parse.getField(4));
        Assert.assertEquals(80, parse.getField(5));
        Assert.assertEquals(0.000022, parse.getField(6));
        Assert.assertEquals(0.026109, parse.getField(7));
        Assert.assertEquals(0.00002, parse.getField(8));
        Assert.assertEquals("200", parse.getField(9));
        Assert.assertEquals("200", parse.getField(10));
        Assert.assertEquals(0L, parse.getField(11));
        Assert.assertEquals(699L, parse.getField(12));
        Assert.assertEquals("GET", parse.getField(13));
        Assert.assertEquals("https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null", parse.getField(14));
        Assert.assertEquals("HTTP/1.1", parse.getField(15));
        Assert.assertEquals("\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\"", parse.getField(16));
        Assert.assertEquals("ECDHE-RSA-AES128-GCM-SHA256", parse.getField(17));
        Assert.assertEquals("TLSv1.2", parse.getField(18));
    }
}
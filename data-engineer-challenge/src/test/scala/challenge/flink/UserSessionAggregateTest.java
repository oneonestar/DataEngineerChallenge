package challenge.flink;

import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import scala.Option;
import scala.collection.immutable.HashSet;

import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class UserSessionAggregateTest {
    private SessionInfo accumulator;
    private UserSessionAggregate userSessionAggregate;
    private Row row;
    private Timestamp timestamp;

    @Before
    public void init() {
        userSessionAggregate = new UserSessionAggregate();
        accumulator = userSessionAggregate.createAccumulator();
        row = new Row(19);
        timestamp = Timestamp.from(Instant.parse("2015-07-22T09:00:00.019143Z"));
        row.setField(0, timestamp);
        row.setField(2, "1.2.3.4");
        row.setField(14, "http://example.com");
    }

    @Test
    public void add() {
        SessionInfo result = userSessionAggregate.add(row, accumulator);
        HashSet<String> hashSet = new HashSet<>();
        hashSet = hashSet.$plus("http://example.com");

        assertEquals("1.2.3.4", result.request_ip());
        assertEquals(hashSet, result.unique_url());
        assertEquals(Option.apply(timestamp), result.earliest_time());
        assertEquals(Option.apply(timestamp), result.latest_time());
        assertEquals(1, result.count());
    }

    @Test
    public void createAccumulator() {
        assertEquals("", accumulator.request_ip());
        assertEquals(new HashSet<String>(), accumulator.unique_url());
        assertEquals(Option.empty(), accumulator.earliest_time());
        assertEquals(Option.empty(), accumulator.latest_time());
        assertEquals(0, accumulator.count());
    }

    @Test
    public void getResult() {
        SessionInfo intermediate = userSessionAggregate.add(row, userSessionAggregate.createAccumulator());
        SessionInfo result = userSessionAggregate.getResult(intermediate);

        HashSet<String> hashSet = new HashSet<>();
        hashSet = hashSet.$plus("http://example.com");

        assertEquals("1.2.3.4", result.request_ip());
        assertEquals(hashSet, result.unique_url());
        assertEquals(Option.apply(timestamp), result.earliest_time());
        assertEquals(Option.apply(timestamp), result.latest_time());
        assertEquals(1, result.count());
    }

    @Test
    public void mergeSameURL() {
        SessionInfo intermediate = userSessionAggregate.add(row, userSessionAggregate.createAccumulator());

        Row anotherRow = new Row(19);
        Timestamp anotherTimestamp = Timestamp.from(Instant.parse("2015-07-22T10:00:00.019143Z"));
        anotherRow.setField(0, anotherTimestamp);
        anotherRow.setField(2, "1.2.3.4");
        anotherRow.setField(14, "http://example.com");
        SessionInfo intermediate2 = userSessionAggregate.add(anotherRow, userSessionAggregate.createAccumulator());

        SessionInfo sessionInfo = userSessionAggregate.merge(intermediate, intermediate2);
        assertEquals(1, sessionInfo.unique_url().size());   // Should be 1 unique count
        assertEquals(2, sessionInfo.count());
        assertEquals("1.2.3.4", sessionInfo.request_ip());
        assertEquals(Option.apply(timestamp), sessionInfo.earliest_time());
        assertEquals(Option.apply(anotherTimestamp), sessionInfo.latest_time());
    }

    @Test
    public void mergeDifferentURL() {
        SessionInfo intermediate = userSessionAggregate.add(row, userSessionAggregate.createAccumulator());

        Row anotherRow = new Row(19);
        anotherRow.setField(0, timestamp);
        anotherRow.setField(2, "1.2.3.4");
        anotherRow.setField(14, "http://helloworld.com");
        SessionInfo intermediate2 = userSessionAggregate.add(anotherRow, userSessionAggregate.createAccumulator());

        SessionInfo sessionInfo = userSessionAggregate.merge(intermediate, intermediate2);
        HashSet<String> hashSet = new HashSet<>();
        hashSet = hashSet.$plus("http://example.com")
                .$plus("http://helloworld.com");

        assertEquals(hashSet, sessionInfo.unique_url());
    }
}
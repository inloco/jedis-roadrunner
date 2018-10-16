package redis.clients.jedis.tests.commands;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class GetGeotargetingsCommandTest extends JedisCommandTestBase {
    private static final String HOST = "localhost";
    private static final Integer PORT = 6330;
    private static final String BUCKET = "bucket";
    private static final long BUCKET_SIZE = 10;
    private static final long BLOCK_SIZE = 4;

    private Jedis jedis;

    private static final String[] geoTargetings = new String[] {
            BUCKET + " 5ab02d2bda651c000c003471 5ab02d2bda651c000c00344e -19.769776 -43.954117 2000",
            BUCKET + " 5ab02d2bda651c000c003472 5ab02d2bda651c000c00344e -19.777717 -43.982016 2000",
            BUCKET + " 5ab02d2bda651c000c003473 5ab02d2bda651c000c00344e -19.745828 -43.936780 2000",
            BUCKET + " 5ab02d2bda651c000c003474 5ab02d2bda651c000c00344e -19.155954 -45.449168 2000",
            BUCKET + " 5ab02d2bda651c000c003475 5ab02d2bda651c000c00344e -18.487074 -47.402992 2000",
            BUCKET + " 5ab02d2bda651c000c003476 5ab02d2bda651c000c00344e -21.768856 -42.534428 2000",
            BUCKET + " 5ab02d2bda651c000c003477 5ab02d2bda651c000c00344e -21.011259 -42.837521 2000",
            BUCKET + " 5ab02d2bda651c000c003478 5ab02d2bda651c000c00344e -18.474505 -42.308141 2000",
            BUCKET + " 5ab02d2bda651c000c003479 5ab02d2bda651c000c00344e -18.823608 -42.705867 2000",
            BUCKET + " 5ab02d2bda651c000c003470 5ab02d2bda651c000c00344e -22.332888 -45.092290 2000",
            BUCKET + " 5ab02d2bda651c000c003416 5ab02d2bda651c000c00344e -16.804675 -42.341585 2000",
            BUCKET + " 5ab02d2bda651c000c003426 5ab02d2bda651c000c00344e -20.866416 -42.245419 2000",
            BUCKET + " 5ab02d2bda651c000c003436 5ab02d2bda651c000c00344e -20.036476 -42.266680 2000",
            BUCKET + " 5ab02d2bda651c000c003446 5ab02d2bda651c000c00344e -19.666991 -48.308671 2000",
            BUCKET + " 5ab02d2bda651c000c003456 5ab02d2bda651c000c00344e -17.397881 -42.731132 2000",
            BUCKET + " 5ab02d2bda651c000c003466 5ab02d2bda651c000c00344e -15.588396 -43.612954 2000",
            BUCKET + " 5ab02d2bda651c000c003476 5ab02d2bda651c000c00344e -17.987421 -46.907232 2000",
            BUCKET + " 5ab02d2bda651c000c003486 5ab02d2bda651c000c00344e -15.702906 -44.028745 2000",
            BUCKET + " 5ab02d2bda651c000c003496 5ab02d2bda651c000c00344e -17.597964 -44.730935 2000",
            BUCKET + " 5ab02d2bda651c000c003406 5ab02d2bda651c000c00344e -17.592505 -44.734022 2000",
            BUCKET + " 5ab02d2bda651c000c003176 5ab02d2bda651c000c00344e -21.553928 -45.439342 2000",
            BUCKET + " 5ab02d2bda651c000c003276 5ab02d2bda651c000c00344e -15.404546 -42.309829 2000",
            BUCKET + " 5ab02d2bda651c000c003376 5ab02d2bda651c000c00344e -20.329418 -46.367387 2000",
            BUCKET + " 5ac404323b3cb4000c00eace 5ac4042f3b3cb4000c00e9b7 -16.493216 -39.071703 10000",
            BUCKET + " 5ac404323b3cb4000c00ead0 5ac4042f3b3cb4000c00e9b7 -9.0147345 -42.688726 10000",
            BUCKET + " 5ac404323b3cb4000c00ead2 5ac4042f3b3cb4000c00e9b7 -12.934989 -38.392579 10000",
            BUCKET + " 5ac404323b3cb4000c00ead4 5ac4042f3b3cb4000c00e9b7 -12.983474 -38.512973 10000",
            BUCKET + " 5ac404323b3cb4000c00ead6 5ac4042f3b3cb4000c00e9b7 -23.032648 -45.560944 10000",
            BUCKET + " 5ac404343b3cb4000c00ec9d 5ac404343b3cb4000c00ec9e -12.577684 -38.007046 10000",
            BUCKET + " 5ac404343b3cb4000c00eca0 5ac404343b3cb4000c00ec9e -12.937157 -38.410444 10000",
            BUCKET + " 5ac404343b3cb4000c00eca2 5ac404343b3cb4000c00ec9e -13.012701 -38.482461 10000",
            BUCKET + " 5ac404343b3cb4000c00eca4 5ac404343b3cb4000c00ec9e -12.981759 -38.464251 10000",
            BUCKET + " 5ac404343b3cb4000c00eca5 5ac404343b3cb4000c00ec9e -12.981759 -38.464251 10000",
    };

    @Before
    public void saveGeoTargetings() {
        try (TestGeotargetingClient testGeotargetingClient = new TestGeotargetingClient(HOST, PORT)) {
            for (String geoTargeting: geoTargetings) {
                assertEquals("OK", testGeotargetingClient.addGeoTargeting(geoTargeting));
            }
        }
    }

    @Before
    public void setUp() {
        jedis = new Jedis(HOST, PORT);
        jedis.flushAll();
        jedis.addBucket(BUCKET, BUCKET_SIZE, BLOCK_SIZE);
    }

    @After
    public void tearDown() {
        jedis.disconnect();
    }

    @Test
    public void firstQuery() {
        Set<String> result = new HashSet<>(jedis.getGeotargetings(BUCKET, -19.75644, -43.94415));
        Set<String> expected = new HashSet<String>() {{
            add("5ab02d2bda651c000c00344e -19.745828 -43.93678 2000 5ab02d2bda651c000c003473");
            add("5ab02d2bda651c000c00344e -19.769776 -43.954117 2000 5ab02d2bda651c000c003471");
        }};
        assertEquals(expected, result);
    }

    @Test
    public void secondQuery() {
        Set<String> result = new HashSet<>(jedis.getGeotargetings(BUCKET, -19.77369, -43.96599));
        Set<String> expected = new HashSet<String>() {{
            add("5ab02d2bda651c000c00344e -19.769776 -43.954117 2000 5ab02d2bda651c000c003471");
            add("5ab02d2bda651c000c00344e -19.777717 -43.982016 2000 5ab02d2bda651c000c003472");
        }};
        assertEquals(expected, result);
    }

    @Test
    public void givenCoordinatesWithMultipleResults_shouldRespectMaximumResponseLengthParamter() {
        List<String> result = jedis.getGeotargetings(BUCKET, -12.93715, -38.41044);
        assertEquals(result.size(), 3);
    }

    @Test
    public void thirdQuery() {
        Set<String> result = new HashSet<>(jedis.getGeotargetings(BUCKET, -12.93715, -38.41044));
        Set<String> expected = new HashSet<String>() {{
            add("5ac404343b3cb4000c00ec9e -12.937157 -38.410444 10000 5ac404343b3cb4000c00eca0");
            add("5ac404343b3cb4000c00ec9e -12.981759 -38.464251 10000 5ac404343b3cb4000c00eca4 5ac404343b3cb4000c00eca5");
            add("5ac4042f3b3cb4000c00e9b7 -12.934989 -38.392579 10000 5ac404323b3cb4000c00ead2");
        }};

        assertEquals(expected, result);
    }

    private static class TestGeotargetingClient extends Connection {
        private final Jedis jedis = new Jedis(HOST, PORT);

        private TestGeotargetingClient(String host, Integer port) {
            super(host, port);
        }

        String addGeoTargeting(String geoTargeting) {
            String[] args = geoTargeting.split(" ");
            String targetingId = args[1];
            String campaignId = args[2];
            double latitude = Double.valueOf(args[3]);
            double longitude = Double.valueOf(args[4]);
            int radius = Integer.valueOf(args[5]);

            return jedis.calcGeotargeting(BUCKET, targetingId, campaignId, latitude, longitude, radius);
        }

        @Override
        public void close() {
            jedis.disconnect();
            super.close();
        }
    }
}

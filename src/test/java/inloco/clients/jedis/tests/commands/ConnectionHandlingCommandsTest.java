package inloco.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import inloco.clients.jedis.BinaryJedis;
import inloco.clients.jedis.HostAndPort;
import inloco.clients.jedis.tests.HostAndPortUtil;

public class ConnectionHandlingCommandsTest extends JedisCommandTestBase {
  protected static HostAndPort hnp = HostAndPortUtil.getRedisServers().get(0);

  @Test
  public void quit() {
    assertEquals("OK", jedis.quit());
  }

  @Test
  public void binary_quit() {
    BinaryJedis bj = new BinaryJedis(hnp.getHost());
    assertEquals("OK", bj.quit());
  }
}

package org.apache.pinot.common.utils;

import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RoundRobinURIProviderTest {

  @Test
  public void testHostAddressRoundRobin()
      throws URISyntaxException, UnknownHostException {

    InetAddress[] testWebAddresses = new InetAddress[]{
        InetAddress.getByAddress("testweb.com", InetAddresses.forString("192.168.3.1").getAddress()),
        InetAddress.getByAddress("testweb.com", InetAddresses.forString("192.168.3.2").getAddress()),
        InetAddress.getByAddress("testweb.com", InetAddresses.forString("192.168.3.3").getAddress())
    };
    InetAddress[] localHostAddresses = new InetAddress[]{
        InetAddress.getByAddress("localhost", InetAddresses.forString("127.0.0.1").getAddress()),
        InetAddress.getByAddress("localhost", InetAddresses.forString("0:0:0:0:0:0:0:1").getAddress())
    };

    MockedStatic<InetAddress> mock = Mockito.mockStatic(InetAddress.class);
    mock.when(() -> InetAddress.getAllByName("localhost")).thenReturn(localHostAddresses);
    mock.when(() -> InetAddress.getAllByName("testweb.com")).thenReturn(testWebAddresses);

    TestCase[] testCases = new TestCase[]{
        new TestCase("http://127.0.0.1", new String[]{"http://127.0.0.1"}),
        new TestCase("http://127.0.0.1/", new String[]{"http://127.0.0.1/"}),
        new TestCase("http://127.0.0.1/?", new String[]{"http://127.0.0.1/?"}),
        new TestCase("http://127.0.0.1/?it=5", new String[]{"http://127.0.0.1/?it=5"}),
        new TestCase("http://127.0.0.1/me/out?it=5", new String[]{"http://127.0.0.1/me/out?it=5"}),
        new TestCase("http://127.0.0.1:20000", new String[]{"http://127.0.0.1:20000"}),
        new TestCase("http://127.0.0.1:20000/", new String[]{"http://127.0.0.1:20000/"}),
        new TestCase("http://127.0.0.1:20000/?", new String[]{"http://127.0.0.1:20000/?"}),
        new TestCase("http://127.0.0.1:20000/?it=5", new String[]{"http://127.0.0.1:20000/?it=5"}),
        new TestCase("http://127.0.0.1:20000/me/out?it=5", new String[]{"http://127.0.0.1:20000/me/out?it=5"}),

        new TestCase("http://localhost", new String[]{"http://127.0.0.1", "http://[0:0:0:0:0:0:0:1]"}),
        new TestCase("http://localhost/", new String[]{"http://127.0.0.1/", "http://[0:0:0:0:0:0:0:1]/"}),
        new TestCase("http://localhost/?", new String[]{"http://127.0.0.1/?", "http://[0:0:0:0:0:0:0:1]/?"}),
        new TestCase("http://localhost/?it=5",
            new String[]{"http://127.0.0.1/?it=5", "http://[0:0:0:0:0:0:0:1]/?it=5"}),
        new TestCase("http://localhost/me/out?it=5",
            new String[]{"http://127.0.0.1/me/out?it=5", "http://[0:0:0:0:0:0:0:1]/me/out?it=5"}),
        new TestCase("http://localhost:20000",
            new String[]{"http://127.0.0.1:20000", "http://[0:0:0:0:0:0:0:1]:20000"}),
        new TestCase("http://localhost:20000/",
            new String[]{"http://127.0.0.1:20000/", "http://[0:0:0:0:0:0:0:1]:20000/"}),
        new TestCase("http://localhost:20000/?",
            new String[]{"http://127.0.0.1:20000/?", "http://[0:0:0:0:0:0:0:1]:20000/?"}),
        new TestCase("http://localhost:20000/?it=5",
            new String[]{"http://127.0.0.1:20000/?it=5", "http://[0:0:0:0:0:0:0:1]:20000/?it=5"}),
        new TestCase("http://localhost:20000/me/out?it=5",
            new String[]{"http://127.0.0.1:20000/me/out?it=5", "http://[0:0:0:0:0:0:0:1]:20000/me/out?it=5"}),

        new TestCase("http://testweb.com",
            new String[]{"http://192.168.3.1", "http://192.168.3.2", "http://192.168.3.3"}),
        new TestCase("http://testweb.com/",
            new String[]{"http://192.168.3.1/", "http://192.168.3.2/", "http://192.168.3.3/"}),
        new TestCase("http://testweb.com/?",
            new String[]{"http://192.168.3.1/?", "http://192.168.3.2/?", "http://192.168.3.3/?"}),
        new TestCase("http://testweb.com/?it=5",
            new String[]{"http://192.168.3.1/?it=5", "http://192.168.3.2/?it=5", "http://192.168.3.3/?it=5"}),
        new TestCase("http://testweb.com/me/out?it=5",
            new String[]{"http://192.168.3.1/me/out?it=5", "http://192.168.3.2/me/out?it=5",
                "http://192.168.3.3/me/out?it=5"}),
        new TestCase("http://testweb.com:20000",
            new String[]{"http://192.168.3.1:20000", "http://192.168.3.2:20000", "http://192.168.3.3:20000"}),
        new TestCase("http://testweb.com:20000/",
            new String[]{"http://192.168.3.1:20000/", "http://192.168.3.2:20000/", "http://192.168.3.3:20000/"}),
        new TestCase("http://testweb.com:20000/?",
            new String[]{"http://192.168.3.1:20000/?", "http://192.168.3.2:20000/?", "http://192.168.3.3:20000/?"}),
        new TestCase("http://testweb.com:20000/?it=5",
            new String[]{"http://192.168.3.1:20000/?it=5", "http://192.168.3.2:20000/?it=5",
                "http://192.168.3.3:20000/?it=5"}),
        new TestCase("http://testweb.com:20000/me/out?it=5",
            new String[]{"http://192.168.3.1:20000/me/out?it=5", "http://192.168.3.2:20000/me/out?it=5",
                "http://192.168.3.3:20000/me/out?it=5"}),

        new TestCase("https://127.0.0.1", new String[]{"https://127.0.0.1"}),
        new TestCase("https://127.0.0.1/", new String[]{"https://127.0.0.1/"}),
        new TestCase("https://127.0.0.1/?", new String[]{"https://127.0.0.1/?"}),
        new TestCase("https://127.0.0.1/?it=5", new String[]{"https://127.0.0.1/?it=5"}),
        new TestCase("https://127.0.0.1/me/out?it=5", new String[]{"https://127.0.0.1/me/out?it=5"}),
        new TestCase("https://127.0.0.1:20000", new String[]{"https://127.0.0.1:20000"}),
        new TestCase("https://127.0.0.1:20000/", new String[]{"https://127.0.0.1:20000/"}),
        new TestCase("https://127.0.0.1:20000/?", new String[]{"https://127.0.0.1:20000/?"}),
        new TestCase("https://127.0.0.1:20000/?it=5", new String[]{"https://127.0.0.1:20000/?it=5"}),
        new TestCase("https://127.0.0.1:20000/me/out?it=5",
            new String[]{"https://127.0.0.1:20000/me/out?it=5"}),

        new TestCase("https://localhost", new String[]{"https://127.0.0.1", "https://[0:0:0:0:0:0:0:1]"}),
        new TestCase("https://localhost/", new String[]{"https://127.0.0.1/", "https://[0:0:0:0:0:0:0:1]/"}),
        new TestCase("https://localhost/?", new String[]{"https://127.0.0.1/?", "https://[0:0:0:0:0:0:0:1]/?"}),
        new TestCase("https://localhost/?it=5",
            new String[]{"https://127.0.0.1/?it=5", "https://[0:0:0:0:0:0:0:1]/?it=5"}),
        new TestCase("https://localhost/me/out?it=5",
            new String[]{"https://127.0.0.1/me/out?it=5", "https://[0:0:0:0:0:0:0:1]/me/out?it=5"}),
        new TestCase("https://localhost:20000",
            new String[]{"https://127.0.0.1:20000", "https://[0:0:0:0:0:0:0:1]:20000"}),
        new TestCase("https://localhost:20000/",
            new String[]{"https://127.0.0.1:20000/", "https://[0:0:0:0:0:0:0:1]:20000/"}),
        new TestCase("https://localhost:20000/?",
            new String[]{"https://127.0.0.1:20000/?", "https://[0:0:0:0:0:0:0:1]:20000/?"}),
        new TestCase("https://localhost:20000/?it=5",
            new String[]{"https://127.0.0.1:20000/?it=5", "https://[0:0:0:0:0:0:0:1]:20000/?it=5"}),
        new TestCase("https://localhost:20000/me/out?it=5",
            new String[]{"https://127.0.0.1:20000/me/out?it=5", "https://[0:0:0:0:0:0:0:1]:20000/me/out?it=5"}),

        new TestCase("https://testweb.com",
            new String[]{"https://192.168.3.1", "https://192.168.3.2", "https://192.168.3.3"}),
        new TestCase("https://testweb.com/",
            new String[]{"https://192.168.3.1/", "https://192.168.3.2/", "https://192.168.3.3/"}),
        new TestCase("https://testweb.com/?",
            new String[]{"https://192.168.3.1/?", "https://192.168.3.2/?", "https://192.168.3.3/?"}),
        new TestCase("https://testweb.com/?it=5",
            new String[]{"https://192.168.3.1/?it=5", "https://192.168.3.2/?it=5", "https://192.168.3.3/?it=5"}),
        new TestCase("https://testweb.com/me/out?it=5",
            new String[]{"https://192.168.3.1/me/out?it=5", "https://192.168.3.2/me/out?it=5",
                "https://192.168.3.3/me/out?it=5"}),
        new TestCase("https://testweb.com:20000",
            new String[]{"https://192.168.3.1:20000", "https://192.168.3.2:20000", "https://192.168.3.3:20000"}),
        new TestCase("https://testweb.com:20000/",
            new String[]{"https://192.168.3.1:20000/", "https://192.168.3.2:20000/", "https://192.168.3.3:20000/"}),
        new TestCase("https://testweb.com:20000/?",
            new String[]{"https://192.168.3.1:20000/?", "https://192.168.3.2:20000/?", "https://192.168.3.3:20000/?"}),
        new TestCase("https://testweb.com:20000/?it=5",
            new String[]{"https://192.168.3.1:20000/?it=5", "https://192.168.3.2:20000/?it=5",
                "https://192.168.3.3:20000/?it=5"}),
        new TestCase("https://testweb.com:20000/me/out?it=5",
            new String[]{"https://192.168.3.1:20000/me/out?it=5", "https://192.168.3.2:20000/me/out?it=5",
                "https://192.168.3.3:20000/me/out?it=5"}),
    };

    for (TestCase testCase : testCases) {
      String uri = testCase._originalUri;
      RoundRobinURIProvider uriProvider = new RoundRobinURIProvider(new URI(uri));
      int n = testCase._expectedUris.length;
      for (int i = 0; i < 2 * n; i++) {
        String expectedUri = testCase._expectedUris[i % n];
        Assert.assertEquals(uriProvider.next().toString(), expectedUri);
      }
    }
  }

  static class TestCase {
    String _originalUri;
    String[] _expectedUris;

    TestCase(String originalUri, String[] expectedUris) {
      _originalUri = originalUri;
      _expectedUris = expectedUris;
    }
  }
}

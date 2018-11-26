package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.naming.CommunicationException;
import javax.naming.directory.SearchControls;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.security.LdapGroupsMapping.LDAP_NUM_RETRIES_BEFORE_FAILOVER_KEY;
import static org.apache.hadoop.security.LdapGroupsMapping.LDAP_NUM_RETRIES_KEY;
import static org.apache.hadoop.security.LdapGroupsMapping.LDAP_URL_KEY;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestLdapGroupsMappingWithFailover
    extends TestLdapGroupsMappingBase {

  private static final String TEST_USER_NAME = "some_user";

  /**
   * Test that when disabled, we will retry the configured number
   * of times using the same LDAP server.
   */
  @Test
  public void testDoesNotFailoverWhenDisabled() throws Exception {
    final int numRetries = 3;
    Configuration conf = getBaseConf();
    conf.setStrings(LDAP_URL_KEY, "ldap://test", "ldap://test1",
        "ldap://test2");
    DummyLdapCtxFactory.setExpectedLdapUrl("ldap://test");
    conf.setInt(LDAP_NUM_RETRIES_KEY, numRetries);
    conf.setInt(LDAP_NUM_RETRIES_BEFORE_FAILOVER_KEY, numRetries);

    when(getContext().search(anyString(), anyString(), any(Object[].class),
        any(SearchControls.class))).thenThrow(new CommunicationException());

    LdapGroupsMapping groupsMapping = getGroupsMapping();
    groupsMapping.setConf(conf);

    List<String> groups = groupsMapping.getGroups(TEST_USER_NAME);
    assertTrue(groups.isEmpty());

    // Test that we retried 3 times with the same server
    verify(getContext(), times(numRetries)).search(anyString(), anyString(),
        any(Object[].class), any(SearchControls.class));
  }

  /**
   * Test that when configured, we will make the specified amount of
   * retries using one ldap url before failing over to the next one.
   *
   * This also tests that we wrap back to the first server
   * if we've tried them all.
   */
  @Test
  public void testFailover() throws Exception {
    Queue<String> ldapUrls = new LinkedList<>();
    ldapUrls.add("ldap://test");
    ldapUrls.add("ldap://test1");
    ldapUrls.add("ldap://test2");

    final int numOverallRetries = 12;
    final int numRetriesBeforeFailover = 2;

    Configuration conf = getBaseConf();
    conf.setStrings(LDAP_URL_KEY, String.join(",", ldapUrls));
    conf.setInt(LDAP_NUM_RETRIES_KEY, numOverallRetries);
    conf.setInt(LDAP_NUM_RETRIES_BEFORE_FAILOVER_KEY, numRetriesBeforeFailover);

    // Set the first expected url and add it back to the queue
    String nextLdapUrl = ldapUrls.remove();
    DummyLdapCtxFactory.setExpectedLdapUrl(nextLdapUrl);
    ldapUrls.add(nextLdapUrl);

    // Number of retries using a single ldap server url
    final AtomicInteger serverRetries = new AtomicInteger(
        numRetriesBeforeFailover);

    when(getContext().search(anyString(), anyString(), any(Object[].class),
        any(SearchControls.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        if (serverRetries.get() == 0) {
          // Take the head of the queue and re-queue it to the back
          String nextLdapUrl = ldapUrls.remove();
          ldapUrls.add(nextLdapUrl);

          DummyLdapCtxFactory.setExpectedLdapUrl(nextLdapUrl);
          serverRetries.set(numRetriesBeforeFailover);
        } else {
          serverRetries.decrementAndGet();
        }
        throw new CommunicationException();
      }
    });

    LdapGroupsMapping groupsMapping = getGroupsMapping();
    groupsMapping.setConf(conf);

    List<String> groups = groupsMapping.getGroups(TEST_USER_NAME);
    assertTrue(groups.isEmpty());

    // Test that we retried 6 times overall
    verify(getContext(), times(numOverallRetries)).search(anyString(),
        anyString(), any(Object[].class), any(SearchControls.class));
  }
}

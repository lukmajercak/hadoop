/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests to verify Safe Mode with replication allowed correctness.
 */
public class TestSafeModeWithBlockReplication {

  private static final int BLOCK_SIZE = 1024;
  private static final Path TEST_PATH = new Path("/test");

  Configuration conf;
  MiniDFSCluster cluster;
  DistributedFileSystem dfs;
  ClientProtocol dfsClient;

  @Before
  public void startUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();

    BlockManagerTestUtil.setWritingPrefersLocalNode(
      cluster.getNamesystem().getBlockManager(), false);

    dfsClient = new DFSClient(new InetSocketAddress("localhost",
      cluster.getNameNodePort()),
      conf).getNamenode();
  }

  @After
  public void tearDown() throws IOException {
    if (dfs != null) {
      dfs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /*
   * Tests some utility methods that surround the SafeMode state.
   * @throws IOException when there's an issue connecting to the test DFS.
   */
  @Test
  public void testSafeModeUtils() throws IOException {
    // Enter Safe Mode with replication/deletion allowed
    enterSafeMode(true);

    // Exit Safe Mode
    leaveSafeMode();

    // Should be able to override default Safe Mode (one that does not
    // allow replication) with one that does
    enterSafeMode(false);
    enterSafeMode(true);
    assertTrue(cluster.getNamesystem().allowsBlockReplication());
  }

  /**
   * NameNode should continue block replicaton work.
   */
  @Test(timeout=40000)
  public void testReplicationWhileInSafeMode() throws IOException {
    // Create a file
    DFSTestUtil.createFile(cluster.getFileSystem(), TEST_PATH,
      3*BLOCK_SIZE, (short)2, 1L);

    // Enter Safe Mode with replication/deletion allowed
    enterSafeMode(true);

    // Assert that we only have 1 replica
    waitForBlockReplication(1);

    // Start new datanode
    cluster.startDataNodes(conf, 1, true, null, null);

    // Wait for replication
    waitForBlockReplication(2);
  }

  /**
   * NameNode should continue block invalidation/deletion work.
   */
  @Test(timeout=40000)
  public void testDeletionWhileInSafeMode() throws IOException {
    // Start new datanode
    cluster.startDataNodes(conf, 1, true, null, null);

    // Create a file
    DFSTestUtil.createFile(cluster.getFileSystem(), TEST_PATH,
      3*BLOCK_SIZE, (short)2, 1L);

    // Wait for replication
    waitForBlockReplication(2);

    // Set file replication to 1
    NameNodeAdapter.setReplication(cluster.getNamesystem(),
      TEST_PATH.toUri().getPath(), (short) 1);

    // Enter Safe Mode with replication/deletion allowed
    enterSafeMode(true);

    // Assert that we only have 1 replica
    waitForBlockReplication(1);
  }


  // Waits for all of the blocks to have expected replication
  private void waitForBlockReplication(int expected)
    throws IOException {

    String filename = TEST_PATH.toUri().getPath();

    // Wait for all the blocks to be replicated;
    boolean replOk = false;
    while (!replOk) {
      replOk = true;

      LocatedBlocks blocks = dfsClient.getBlockLocations(filename, 0,
        Long.MAX_VALUE);

      for (Iterator<LocatedBlock> iter = blocks.getLocatedBlocks().iterator();
           iter.hasNext();) {
        LocatedBlock block = iter.next();
        replOk &= block.getLocations().length == expected;
      }

      try {
        Thread.sleep(200);
      } catch (InterruptedException ignored) {}
    }
  }

  private void enterSafeMode(boolean allowReplication) throws IOException {
    SafeModeAction action = allowReplication ?
      SafeModeAction.SAFEMODE_ENTER_RECOVERY :
      SafeModeAction.SAFEMODE_ENTER;
    assertTrue("Could not enter SM", dfs.setSafeMode(action));
    assertTrue(dfs.isInSafeMode());
  }

  private void leaveSafeMode() throws IOException {
    assertFalse("Could not leave SM",
      dfs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE));
    assertFalse(dfs.isInSafeMode());
  }
}

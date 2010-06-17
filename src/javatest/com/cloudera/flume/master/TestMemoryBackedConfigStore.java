/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.master;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.cloudera.flume.conf.thrift.FlumeConfigData;

import junit.framework.TestCase;

public class TestMemoryBackedConfigStore extends TestCase {
  /**
   * Test that set and get work correctly, and that save and load work
   * correctly.
   */
  public void testGetSetSaveLoad() throws IOException {
    File tmp = File.createTempFile("test-flume", "");
    tmp.delete();
    tmp.deleteOnExit();
    MemoryBackedConfigStore store = new MemoryBackedConfigStore();
    ConfigManager manager = new ConfigManager(store);
    manager.setConfig("foo", "my-test-flow", "bar", "baz");
    FlumeConfigData data = manager.getConfig("foo");
    assertEquals(data.getSinkConfig(), "baz");
    assertEquals(data.getSourceConfig(), "bar");

    manager.saveConfig(tmp.getAbsolutePath());

    manager = new ConfigManager(new MemoryBackedConfigStore());
    manager.loadConfig(tmp.getAbsolutePath());
    data = manager.getConfig("foo");
    assertEquals(data.getSinkConfig(), "baz");
    assertEquals(data.getSourceConfig(), "bar");
  }

  /**
   * Test that set and get work correctly (do not do persistence here.)
   */
  public void testNodes() throws IOException {
    File tmp = File.createTempFile("test-flume", "");
    tmp.delete();
    tmp.deleteOnExit();
    MemoryBackedConfigStore store = new MemoryBackedConfigStore();
    ConfigManager manager = new ConfigManager(store);

    manager.addLogicalNode("physical", "logical1");
    manager.addLogicalNode("physical", "logical2");
    manager.addLogicalNode("physical", "logical3");
    manager.addLogicalNode("p2", "l2");
    manager.addLogicalNode("p3", "l3");

    List<String> lns = manager.getLogicalNode("physical");
    assertTrue(lns.contains("logical1"));
    assertTrue(lns.contains("logical2"));
    assertTrue(lns.contains("logical3"));

    assertTrue(manager.getLogicalNode("p2").contains("l2"));
    assertTrue(manager.getLogicalNode("p3").contains("l3"));

  }

  /**
   * Test unmap all work correctly (do not do persistence here.)
   */
  public void testUnmapAllNodes() throws IOException {
    File tmp = File.createTempFile("test-flume", "");
    tmp.delete();
    tmp.deleteOnExit();
    MemoryBackedConfigStore store = new MemoryBackedConfigStore();
    ConfigManager manager = new ConfigManager(store);

    manager.addLogicalNode("physical", "logical1");
    manager.addLogicalNode("physical", "logical2");
    manager.addLogicalNode("physical", "logical3");
    manager.addLogicalNode("p2", "l2");
    manager.addLogicalNode("p3", "l3");

    manager.unmapAllLogicalNodes();

    List<String> lns = manager.getLogicalNode("physical");
    assertFalse(lns.contains("logical1"));
    assertFalse(lns.contains("logical2"));
    assertFalse(lns.contains("logical3"));

    assertFalse(manager.getLogicalNode("p2").contains("l2"));
    assertFalse(manager.getLogicalNode("p3").contains("l3"));

  }
}
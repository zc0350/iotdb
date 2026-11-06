/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.relational.it.mqtt;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.EnvUtils;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import org.apache.tsfile.read.common.Field;
import org.awaitility.Awaitility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Checks that the MQTT subscriber service writes what an external broker delivers into the table
 * model. The broker here is a Moquette server started inside the test JVM, so nothing IoTDB itself
 * listens on is involved: {@code enable_mqtt_service} stays off and the subscriber is the only path
 * that can produce the asserted row.
 *
 * <p>Only the standalone environment is covered. In a cluster every DataNode would subscribe with
 * its own client id and MQTT 3.1 has no shared subscription, so each row would be written once per
 * DataNode.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBMQTTSubscriberIT {

  private static final String DATABASE = "mqttsubtest";
  private static final String TOPIC = DATABASE + "/myTopic";
  private static final String PAYLOAD = "test1,tag1=t1,tag2=t2 field1=1,field2=1f,field3=1i32 1";

  private static final String[] COLUMN_NAMES =
      new String[] {"tag1", "tag2", "field1", "field2", "field3"};

  private static Server broker;
  private static BlockingConnection publisher;

  @BeforeClass
  public static void setUp() throws Exception {
    // The broker has to be listening before the DataNode starts its subscriber, so the port is
    // taken from the framework's allocator rather than from a node wrapper, which does not exist
    // yet. The allocator also keeps the port out of the way of the nodes and of other forks.
    int port = EnvUtils.searchAvailablePorts()[0];

    Properties brokerProperties = new Properties();
    brokerProperties.setProperty(BrokerConstants.HOST_PROPERTY_NAME, "127.0.0.1");
    brokerProperties.setProperty(BrokerConstants.PORT_PROPERTY_NAME, String.valueOf(port));
    brokerProperties.setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "true");
    brokerProperties.setProperty(BrokerConstants.PERSISTENCE_ENABLED_PROPERTY_NAME, "false");
    brokerProperties.setProperty(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, "true");
    broker = new Server();
    broker.startServer(
        new MemoryConfig(brokerProperties), Collections.emptyList(), null, null, null);

    MQTT mqtt = new MQTT();
    mqtt.setHost("127.0.0.1", port);
    // Kept within the 23 characters an MQTT 3.1 broker has to accept.
    mqtt.setClientId("iotdb-mqtt-it-pub");
    mqtt.setConnectAttemptsMax(3);
    mqtt.setCleanSession(true);
    publisher = mqtt.blockingConnection();
    publisher.connect();

    Properties properties = new Properties();
    properties.setProperty("enable_mqtt_subscriber_service", "true");
    properties.setProperty("mqtt_subscriber_broker_host", "127.0.0.1");
    properties.setProperty("mqtt_subscriber_broker_port", String.valueOf(port));
    properties.setProperty("mqtt_subscriber_topics", DATABASE + "/#");
    properties.setProperty("mqtt_subscriber_qos", "1");
    properties.setProperty("mqtt_subscriber_payload_formatter", "line");
    properties.setProperty("mqtt_subscriber_iotdb_user", "root");
    properties.setProperty("mqtt_subscriber_iotdb_password", SessionConfig.DEFAULT_PASSWORD);
    // Keep the built-in broker off: it would write the same rows through another path.
    properties.setProperty("enable_mqtt_service", "false");
    ((MppCommonConfig) EnvFactory.getEnv().getConfig().getDataNodeCommonConfig())
        .updateProperties(properties);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      if (publisher != null) {
        publisher.disconnect();
      }
    } finally {
      if (broker != null) {
        broker.stopServer();
      }
      EnvFactory.getEnv().cleanClusterEnvironment();
    }
  }

  @Test
  public void testSubscribedMessageIsWrittenToTable() throws Exception {
    try (final ITableSession session =
        EnvFactory.getEnv().getTableSessionConnectionWithDB(DATABASE)) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      Awaitility.await()
          .atMost(3, TimeUnit.MINUTES)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(
              () -> {
                publisher.publish(TOPIC, PAYLOAD.getBytes(), QoS.AT_LEAST_ONCE, false);
                try (final SessionDataSet dataSet =
                    session.executeQueryStatement(
                        "select tag1,tag2,field1,field2,field3 from test1 where time = 1")) {
                  if (dataSet.getColumnNames().size() != COLUMN_NAMES.length
                      || !dataSet.hasNext()) {
                    return false;
                  }
                  List<Field> fields = dataSet.next().getFields();
                  assertEquals("t1", fields.get(0).getStringValue());
                  assertEquals("t2", fields.get(1).getStringValue());
                  assertEquals(1d, fields.get(2).getDoubleV(), 0);
                  assertEquals(1f, fields.get(3).getFloatV(), 0);
                  assertEquals(1, fields.get(4).getIntV(), 0);
                  return true;
                } catch (StatementExecutionException e) {
                  // The table and its columns are created by the first successful insert.
                  if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                    return false;
                  }
                  throw e;
                }
              });
    }
  }
}

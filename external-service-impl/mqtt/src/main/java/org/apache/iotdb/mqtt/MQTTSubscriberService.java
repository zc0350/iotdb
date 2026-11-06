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

package org.apache.iotdb.mqtt;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.protocol.session.MqttClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.externalservice.api.IExternalService;
import org.apache.iotdb.mqtt.i18n.MqttMessages;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import io.netty.buffer.Unpooled;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Promise;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * IoTDB connects to an external MQTT broker as a client and writes every message it receives into
 * the table model, instead of waiting for devices to publish to the built-in broker.
 *
 * <p>The service starts on each DataNode whose local configuration enables it. MQTT 3.1 has no
 * shared subscription, so several subscribers of the same topic each receive a copy of every
 * message: keep {@code enable_mqtt_subscriber_service} off while more than one DataNode is running,
 * otherwise the same message is written once per DataNode.
 */
public class MQTTSubscriberService implements IExternalService {

  private static final Logger LOG = LoggerFactory.getLogger(MQTTSubscriberService.class);

  /**
   * MQTT 3.1 brokers must accept client ids of at most 23 characters made of letters, digits and
   * hyphens, and this client only speaks 3.1, so the prefix leaves room for the node id.
   */
  private static final String CLIENT_ID_PREFIX = "iotdb-mqtt-sub-";

  /** How long one receive call waits before it re-checks whether the service is still running. */
  private static final long MESSAGE_POLL_TIMEOUT_MS = 500L;

  private static final long MESSAGE_ACK_TIMEOUT_MS = 5_000L;

  private static final long INITIAL_RETRY_BACK_OFF_MS = 1_000L;

  /** Same ceiling as the client library uses for its own transport reconnects. */
  private static final long MAX_RETRY_BACK_OFF_MS = 30_000L;

  /** Login retries are bounded so that a wrong password cannot lock the account out. */
  private static final int MAX_LOGIN_FAILURES = 3;

  private static final long CONSUMER_JOIN_TIMEOUT_MS = 10_000L;

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final SessionManager sessionManager = SessionManager.getInstance();
  private final TableMessageInserter tableMessageInserter = new TableMessageInserter();

  private volatile boolean running = false;
  private Thread consumerThread;

  @Override
  public void start() {
    int qosLevel = config.getMqttSubscriberQoS();
    if (qosLevel < 0 || qosLevel > 2) {
      throw new RuntimeException(
          String.format(
              MqttMessages.EXCEPTION_THE_MQTT_SUBSCRIBER_QOS_MUST_BE_0_1_OR_2_BUT_IS_ARG_DDB4A981,
              qosLevel));
    }

    String formatterName = config.getMqttSubscriberPayloadFormatter();
    PayloadFormatter payloadFormat = PayloadFormatManager.getPayloadFormat(formatterName);
    if (!PayloadFormatter.TABLE_TYPE.equals(payloadFormat.getType())) {
      throw new RuntimeException(
          String.format(
              MqttMessages
                  .EXCEPTION_THE_MQTT_SUBSCRIBER_PAYLOAD_FORMATTER_ARG_DOES_NOT_WRITE_THE_TABLE_MODEL_92690DCD,
              formatterName));
    }

    Topic[] topics = buildTopics(qosLevel);
    if (topics.length == 0) {
      throw new RuntimeException(
          MqttMessages
              .EXCEPTION_THE_MQTT_SUBSCRIBER_SERVICE_IS_ENABLED_BUT_MQTT_SUBSCRIBER_TOPICS_IS_EMPTY_436FA304);
    }

    LOG.info(
        MqttMessages.LOG_START_MQTT_SUBSCRIBER_SERVICE_BROKER_ARG_ARG_TOPICS_ARG_BA1FA8CC,
        config.getMqttSubscriberBrokerHost(),
        config.getMqttSubscriberBrokerPort(),
        config.getMqttSubscriberTopics());

    running = true;
    consumerThread = new Thread(() -> consume(payloadFormat, topics), "MQTT-Subscriber-Consumer");
    consumerThread.setDaemon(true);
    consumerThread.start();
  }

  @Override
  public void stop() {
    LOG.info(MqttMessages.LOG_STOPPING_IOTDB_MQTT_SUBSCRIBER_SERVICE_BC379BB3);
    running = false;
    if (consumerThread != null) {
      consumerThread.interrupt();
      try {
        consumerThread.join(CONSUMER_JOIN_TIMEOUT_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    LOG.info(MqttMessages.LOG_IOTDB_MQTT_SUBSCRIBER_SERVICE_STOPPED_246A2235);
  }

  private Topic[] buildTopics(int qosLevel) {
    List<Topic> topics = new ArrayList<>();
    for (String topic : config.getMqttSubscriberTopics().split(",")) {
      String name = topic.trim();
      if (!name.isEmpty()) {
        topics.add(new Topic(name, toQoS(qosLevel)));
      }
    }
    return topics.toArray(new Topic[0]);
  }

  private static QoS toQoS(int qosLevel) {
    switch (qosLevel) {
      case 0:
        return QoS.AT_MOST_ONCE;
      case 1:
        return QoS.AT_LEAST_ONCE;
      default:
        return QoS.EXACTLY_ONCE;
    }
  }

  private void consume(PayloadFormatter payloadFormat, Topic[] topics) {
    final String host = config.getMqttSubscriberBrokerHost();
    final int port = config.getMqttSubscriberBrokerPort();
    final int qosLevel = config.getMqttSubscriberQoS();

    BlockingConnection connection = null;
    MqttClientSession session = null;
    long backOffMs = INITIAL_RETRY_BACK_OFF_MS;
    int loginFailures = 0;

    try {
      while (running) {
        try {
          connection = openConnection(host, port);
          if (session == null) {
            session = login();
          }
          connection.subscribe(topics);
          backOffMs = INITIAL_RETRY_BACK_OFF_MS;
          loginFailures = 0;
          LOG.info(
              MqttMessages
                  .LOG_THE_MQTT_SUBSCRIBER_SUBSCRIBED_TO_THE_BROKER_ARG_ARG_TOPICS_ARG_85C2D4C7,
              host,
              port,
              config.getMqttSubscriberTopics());

          while (running) {
            org.fusesource.mqtt.client.Message message =
                connection.receive(MESSAGE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (message != null) {
              consumeMessage(message, payloadFormat, session, qosLevel);
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (LoginFailedException e) {
          loginFailures++;
          if (loginFailures >= MAX_LOGIN_FAILURES) {
            LOG.error(
                MqttMessages
                    .LOG_THE_MQTT_SUBSCRIBER_SERVICE_STOPS_RETRYING_AFTER_ARG_CONSECUTIVE_LOGIN_FAILURES_AS_USER_ARG_A2425C8C,
                loginFailures,
                config.getMqttSubscriberIoTDBUser(),
                e);
            return;
          }
          closeQuietly(connection);
          connection = null;
          backOffMs = waitBeforeRetry(host, port, backOffMs, e);
        } catch (Exception e) {
          closeQuietly(connection);
          connection = null;
          backOffMs = waitBeforeRetry(host, port, backOffMs, e);
        }
      }
    } finally {
      closeQuietly(connection);
      if (session != null) {
        logout(session);
      }
    }
  }

  /**
   * The client library reconnects the transport itself and re-sends the subscriptions, so reaching
   * this method means the connection is gone for good and a fresh one has to be established.
   */
  private long waitBeforeRetry(String host, int port, long backOffMs, Exception e) {
    if (!running) {
      return backOffMs;
    }
    LOG.warn(
        MqttMessages
            .LOG_THE_MQTT_SUBSCRIBER_CONNECTION_TO_THE_BROKER_ARG_ARG_FAILED_RETRYING_IN_ARG_MS_REASON_6694CC00,
        host,
        port,
        backOffMs,
        e);
    try {
      Thread.sleep(backOffMs);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
    return Math.min(backOffMs * 2, MAX_RETRY_BACK_OFF_MS);
  }

  private BlockingConnection openConnection(String host, int port) throws Exception {
    MQTT mqtt = new MQTT();
    mqtt.setHost(host, port);
    mqtt.setClientId(CLIENT_ID_PREFIX + config.getDataNodeId());
    // Keep the broker session so that messages published while this node was disconnected are
    // delivered again; they were not acknowledged, so they were never written to IoTDB.
    mqtt.setCleanSession(false);
    String user = config.getMqttSubscriberBrokerUser();
    if (!user.isEmpty()) {
      mqtt.setUserName(user);
      mqtt.setPassword(config.getMqttSubscriberBrokerPassword());
    }

    BlockingConnection connection = mqtt.blockingConnection();
    connection.connect();
    return connection;
  }

  private MqttClientSession login() {
    MqttClientSession mqttClientSession =
        new MqttClientSession(CLIENT_ID_PREFIX + config.getDataNodeId());
    BasicOpenSessionResp resp =
        sessionManager.login(
            mqttClientSession,
            config.getMqttSubscriberIoTDBUser(),
            config.getMqttSubscriberIoTDBPassword(),
            ZoneId.systemDefault().toString(),
            TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3,
            IoTDBConstant.ClientVersion.V_1_0,
            SqlDialect.TABLE);
    if (resp.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new LoginFailedException(
          String.format(
                  MqttMessages
                      .EXCEPTION_THE_MQTT_SUBSCRIBER_FAILED_TO_LOGIN_TO_IOTDB_AS_USER_ARG_3BA5B995,
                  config.getMqttSubscriberIoTDBUser())
              + resp.getMessage());
    }
    sessionManager.registerSessionForMqtt(mqttClientSession);
    return mqttClientSession;
  }

  private void consumeMessage(
      org.fusesource.mqtt.client.Message message,
      PayloadFormatter payloadFormat,
      MqttClientSession session,
      int qosLevel)
      throws Exception {
    String topic = message.getTopic();
    byte[] payload = message.getPayload();
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          MqttMessages.LOG_RECEIVE_SUBSCRIBE_MESSAGE_TOPIC_ARG_PAYLOAD_LENGTH_ARG_78ECED0A,
          topic,
          payload.length);
    }

    try {
      List<Message> messages = payloadFormat.format(topic, Unpooled.wrappedBuffer(payload));
      if (messages != null) {
        for (Message formatted : messages) {
          if (formatted != null) {
            tableMessageInserter.insert((TableMessage) formatted, session);
          }
        }
      }
    } catch (RuntimeException e) {
      // Acknowledge anyway: leaving the message unacknowledged would make the broker redeliver it
      // forever. The warning below records the reason.
      LOG.warn(
          MqttMessages
              .LOG_THE_MQTT_SUBSCRIBER_FAILED_TO_PROCESS_THE_MESSAGE_ON_TOPIC_ARG_REASON_4567D12D,
          topic,
          e);
    } finally {
      if (qosLevel > 0) {
        Promise<Void> acked = new Promise<>();
        message.ack(acked);
        acked.await(MESSAGE_ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      }
    }
  }

  private void closeQuietly(BlockingConnection connection) {
    if (connection == null || !connection.isConnected()) {
      return;
    }
    try {
      connection.disconnect();
    } catch (Exception e) {
      // The failure that made this connection unnecessary is logged by the caller.
    }
  }

  private void logout(MqttClientSession session) {
    sessionManager.removeCurrSessionForMqtt(session);
    sessionManager.closeSession(session, Coordinator.getInstance()::cleanupQueryExecution);
  }

  private static class LoginFailedException extends RuntimeException {
    LoginFailedException(String message) {
      super(message);
    }
  }
}

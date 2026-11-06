/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.mqtt.i18n;

public final class MqttMessages {

  // --- LinePayloadFormatter ---
  public static final String INVALID_LINE_PROTOCOL = "行协议格式无效，行内容：{}";
  public static final String TAGS_ERROR = "标签格式错误，行内容：{}";
  public static final String ATTRIBUTES_ERROR = "属性格式错误，行内容：{}";
  public static final String FIELDS_ERROR = "字段格式错误，行内容：{}";
  public static final String TIMESTAMP_ERROR = "时间戳格式错误，行内容：{}";

  // --- MPPPublishHandler ---
  public static final String ON_PUBLISH_EXCEPTION =
      "onPublish 执行异常，消息为 [{}]，错误：";
  public static final String PROCESS_RESULT = "处理结果：{}";

  // --- MQTTService ---
  public static final String SERVER_START_EXCEPTION = "启动服务器时发生异常";
  public static final String STOPPING_MQTT_SERVICE = "正在停止 IoTDB MQTT 服务...";
  public static final String MQTT_SERVICE_STOPPED = "IoTDB MQTT 服务已停止。";

  // --- PayloadFormatManager ---
  public static final String MQTT_DIR = "mqttDir：{}";
  public static final String PAYLOAD_FORMAT_MANAGER_INIT_ERROR =
      "MQTT PayloadFormatManager init() 出错。";
  public static final String FORMATTER_IS_NULL = "PayloadFormatManager()，formatter 为 null。";
  public static final String FIND_MQTT_PLUGIN =
      "PayloadFormatManager()，找到 MQTT Payload 插件 {}。";
  public static final String MQTT_PLUGIN_JAR_URLS = "MQTT 插件 jarURLs：{}";
  public static final String UNKNOWN_PAYLOAD_FORMAT_NAMED = "未知 payload 格式名称：";

  // --- JSONPayloadFormatter ---
  public static final String PAYLOAD_INVALID = "payload 无效";

  private MqttMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_LINE_PATTERN_PARSING_FAILS_FAILED_LINE_MESSAGE_ARG_EXCEPTION_6EFB0EE2 = "行模式解析失败，失败的行消息为 {}，异常为";
  public static final String LOG_CONNECTION_REFUSED_CLIENT_ID_MISSING_EMPTY_VALID_CLIENT_ID_REQUIRED_A566DC15 = "连接被拒绝：client_id 缺失或为空。建立连接需要有效的 client_id。";
  public static final String LOG_RECEIVE_PUBLISH_MESSAGE_CLIENTID_ARG_USERNAME_ARG_QOS_ARG_TOPIC_7E60C3A6 = "收到 publish 消息。clientId: {}, 用户名: {}, qos: {}, 主题: {}, payload: {}";
  public static final String LOG_MQTT_JSON_INSERT_ERROR_CODE_ARG_MESSAGE_ARG_B1A78FBD = "MQTT JSON 插入错误，code={}，消息={}";
  public static final String LOG_MEET_ERROR_INSERTING_DATABASE_ARG_TABLE_ARG_TAGS_ARG_ATTRIBUTES_173457D5 = "插入数据库 {}、表 {}、标签 {}、属性 {}、字段 {}、时间 {} 时遇到错误，原因：";
  public static final String LOG_MEET_ERROR_INSERTING_DEVICE_ARG_MEASUREMENTS_ARG_AT_TIME_ARG_680D67D2 = "插入设备 {}、测点 {}、时间 {} 时遇到错误，原因：";
  public static final String LOG_START_MQTT_SERVICE_SUCCESSFULLY_LISTENING_IP_ARG_PORT_ARG_47CE46D5 = "MQTT 服务启动成功，监听 IP {}，端口 {}";

  // --- MQTTSubscriberService ---
  public static final String LOG_START_MQTT_SUBSCRIBER_SERVICE_BROKER_ARG_ARG_TOPICS_ARG_BA1FA8CC = "MQTT 订阅服务已启动，Broker {}:{}，主题 {}";
  public static final String LOG_THE_MQTT_SUBSCRIBER_SUBSCRIBED_TO_THE_BROKER_ARG_ARG_TOPICS_ARG_85C2D4C7 = "MQTT 订阅端已订阅 Broker {}:{}，主题 {}";
  public static final String LOG_STOPPING_IOTDB_MQTT_SUBSCRIBER_SERVICE_BC379BB3 = "正在停止 IoTDB MQTT 订阅服务...";
  public static final String LOG_IOTDB_MQTT_SUBSCRIBER_SERVICE_STOPPED_246A2235 = "IoTDB MQTT 订阅服务已停止。";
  public static final String LOG_THE_MQTT_SUBSCRIBER_CONNECTION_TO_THE_BROKER_ARG_ARG_FAILED_RETRYING_IN_ARG_MS_REASON_6694CC00 = "MQTT 订阅端与 Broker {}:{} 的连接失败，{} 毫秒后重试。原因：";
  public static final String LOG_RECEIVE_SUBSCRIBE_MESSAGE_TOPIC_ARG_PAYLOAD_LENGTH_ARG_78ECED0A = "收到订阅消息。主题: {}, 负载长度: {}";
  public static final String LOG_THE_MQTT_SUBSCRIBER_FAILED_TO_PROCESS_THE_MESSAGE_ON_TOPIC_ARG_REASON_4567D12D = "MQTT 订阅端处理主题 {} 上的消息失败。原因：";
  public static final String LOG_THE_MQTT_SUBSCRIBER_SERVICE_STOPS_RETRYING_AFTER_ARG_CONSECUTIVE_LOGIN_FAILURES_AS_USER_ARG_A2425C8C =
      "MQTT 订阅端在连续 {} 次以用户 {} 登录失败后停止重试。";
  public static final String EXCEPTION_THE_MQTT_SUBSCRIBER_SERVICE_IS_ENABLED_BUT_MQTT_SUBSCRIBER_TOPICS_IS_EMPTY_436FA304 = "MQTT 订阅服务已启用，但 mqtt_subscriber_topics 为空。";
  public static final String EXCEPTION_THE_MQTT_SUBSCRIBER_QOS_MUST_BE_0_1_OR_2_BUT_IS_ARG_DDB4A981 = "MQTT 订阅端的 QoS 必须为 0、1 或 2，但为 %s。";
  public static final String EXCEPTION_THE_MQTT_SUBSCRIBER_PAYLOAD_FORMATTER_ARG_DOES_NOT_WRITE_THE_TABLE_MODEL_92690DCD = "MQTT 订阅端的 payload formatter %s 不会写入表模型。";
  public static final String EXCEPTION_THE_MQTT_SUBSCRIBER_FAILED_TO_LOGIN_TO_IOTDB_AS_USER_ARG_3BA5B995 = "MQTT 订阅端以用户 %s 登录 IoTDB 失败。";

}

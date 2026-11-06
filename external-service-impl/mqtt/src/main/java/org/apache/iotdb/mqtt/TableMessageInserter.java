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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.MqttClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.mqtt.i18n.MqttMessages;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Writes a {@link TableMessage} into the table model. Shared by the built-in MQTT broker, which
 * receives published messages, and the MQTT subscriber, which pulls messages from an external
 * broker.
 */
public class TableMessageInserter {

  private static final Logger LOG = LoggerFactory.getLogger(TableMessageInserter.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final SessionManager sessionManager = SessionManager.getInstance();

  public void insert(TableMessage message, MqttClientSession session) {
    TSStatus tsStatus = null;
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(message.getTimestamp());
      InsertTabletStatement insertTabletStatement = constructInsertTabletStatement(message);
      session.setDatabaseName(message.getDatabase().toLowerCase());
      session.setSqlDialect(SqlDialect.TABLE);
      long queryId = sessionManager.requestQueryId();
      SqlParser relationSqlParser = new SqlParser();
      Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTableModel(
                  insertTabletStatement,
                  relationSqlParser,
                  session,
                  queryId,
                  sessionManager.getSessionInfo(session),
                  "",
                  metadata,
                  config.getQueryTimeoutThreshold());

      tsStatus = result.status;
      if (LOG.isDebugEnabled()) {
        LOG.debug(MqttMessages.PROCESS_RESULT, tsStatus);
      }
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && tsStatus.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        LOG.warn(
            MqttMessages.LOG_MQTT_JSON_INSERT_ERROR_CODE_ARG_MESSAGE_ARG_B1A78FBD,
            tsStatus.getCode(),
            tsStatus.getMessage());
      }
    } catch (Exception e) {
      LOG.warn(
          MqttMessages.LOG_MEET_ERROR_INSERTING_DATABASE_ARG_TABLE_ARG_TAGS_ARG_ATTRIBUTES_173457D5,
          message.getDatabase(),
          message.getTable(),
          message.getTagKeys(),
          message.getAttributeKeys(),
          message.getFields(),
          message.getTimestamp(),
          e);
    }
  }

  private InsertTabletStatement constructInsertTabletStatement(TableMessage message) {
    InsertTabletStatement insertStatement = new InsertTabletStatement();
    insertStatement.setDevicePath(new PartialPath(message.getTable(), false));
    List<String> measurements =
        Stream.of(message.getFields(), message.getTagKeys(), message.getAttributeKeys())
            .flatMap(List::stream)
            .collect(Collectors.toList());
    insertStatement.setMeasurements(measurements.toArray(new String[0]));
    long[] timestamps = new long[] {message.getTimestamp()};
    insertStatement.setTimes(timestamps);
    int columnSize = measurements.size();
    int rowSize = 1;

    BitMap[] bitMaps = new BitMap[columnSize];
    Object[] columns =
        Stream.of(message.getValues(), message.getTagValues(), message.getAttributeValues())
            .flatMap(List::stream)
            .toArray(Object[]::new);
    insertStatement.setColumns(columns);
    insertStatement.setBitMaps(bitMaps);
    insertStatement.setRowCount(rowSize);
    insertStatement.setAligned(false);
    insertStatement.setWriteToTable(true);
    TSDataType[] dataTypes = new TSDataType[measurements.size()];
    TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[measurements.size()];
    for (int i = 0; i < message.getFields().size(); i++) {
      dataTypes[i] = message.getDataTypes().get(i);
      columnCategories[i] = TsTableColumnCategory.FIELD;
    }
    for (int i = message.getFields().size();
        i < message.getFields().size() + message.getTagKeys().size();
        i++) {
      dataTypes[i] = TSDataType.STRING;
      columnCategories[i] = TsTableColumnCategory.TAG;
    }
    for (int i = message.getFields().size() + message.getTagKeys().size();
        i
            < message.getFields().size()
                + message.getTagKeys().size()
                + message.getAttributeKeys().size();
        i++) {
      dataTypes[i] = TSDataType.STRING;
      columnCategories[i] = TsTableColumnCategory.ATTRIBUTE;
    }
    insertStatement.setDataTypes(dataTypes);
    insertStatement.setColumnCategories(columnCategories);

    return insertStatement;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.dameng;

import io.debezium.connector.dameng.logminer.LogContent;
import io.debezium.connector.dameng.logminer.LogMiner;
import io.debezium.connector.dameng.logminer.Operation;
import io.debezium.connector.dameng.logminer.parser.DmlParser;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.dameng.logminer.parser.LogMinerDmlParser;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class DamengStreamingChangeEventSource implements StreamingChangeEventSource<DamengOffsetContext> {
    private static final long LOG_MINER_SCAN_INTERVAL = 1000L;
    private final DamengConnectorConfig connectorConfig;
    private final DamengConnection connection;
    private final List<TableId> tableIds;
    private final EventDispatcher<TableId> eventDispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final DamengDatabaseSchema databaseSchema;
    public DamengStreamingChangeEventSource(DamengConnectorConfig connectorConfig,
                                            DamengConnection connection,
                                            List<TableId> tableIds,
                                            EventDispatcher<TableId> eventDispatcher,
                                            ErrorHandler errorHandler,
                                            Clock clock,
                                            DamengDatabaseSchema databaseSchema) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.tableIds = tableIds;
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.databaseSchema = databaseSchema;
    }

    @Override
    public void execute(ChangeEventSourceContext context, DamengOffsetContext offsetContext) {
        Scn startScn = offsetContext.getScn();
        LogMiner logMiner = LogMiner.builder()
            .connection(connection)
            .schemas(tableIds.stream().map(e -> e.schema()).toArray(value -> new String[value]))
            .tables(tableIds.stream().map(e -> e.table()).toArray(value -> new String[value]))
            .previousScn(startScn)
            .consumers(Arrays.asList(new LogContentConsumer(offsetContext)))
            .build();

        try {
            log.info("Start logminer for scn={}", startScn);
            logMiner.startLoop(() -> context.isRunning(), LOG_MINER_SCAN_INTERVAL);
        } catch (Exception e) {
            log.error("Mining session stopped due to the {}", e);
            errorHandler.setProducerThrowable(e);
        } finally {
            log.info("Close logminer startScn={}, offsetContext.getScn()={}", startScn, offsetContext.getScn());
            logMiner.close();
        }
    }

    @RequiredArgsConstructor
    private class LogContentConsumer implements Consumer<LogContent> {
        private final DamengOffsetContext offsetContext;
        private final DmlParser dmlParser = new LogMinerDmlParser();
        @Override
        public void accept(LogContent logContent) {
            switch (Operation.parse(logContent.getOperationCode())) {
                case INSERT:
                case DELETE:
                case UPDATE:
                    TableId tableId = new TableId(null, logContent.getSegOwner(), logContent.getTableName());
                    Table table = databaseSchema.tableFor(tableId);
                    LogMinerDmlEntry dmlEntry = dmlParser.parse(logContent.getSqlRedo(), table, logContent.getXid());

                    offsetContext.event(tableId, clock.currentTime());
                    offsetContext.setScn(logContent.getScn());

                    try {
                        eventDispatcher.dispatchDataChangeEvent(tableId,
                            new DamengDataChangeRecordEmitter(offsetContext, clock, dmlEntry));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    log.debug("Unsupported log operation: " + logContent.getOperation());
            }
        }
    }
}

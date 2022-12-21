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

package io.debezium.connector.dameng.logminer;

import io.debezium.connector.dameng.DamengConnection;
import io.debezium.connector.dameng.Scn;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Slf4j
@Builder
public class LogMiner implements Closeable {
    private static final int ERROR_CODE_FOR_NOT_ACTIVE_LOGMINER = -2846;
    @NonNull
    private final DamengConnection connection;
    @NonNull
    private final Scn previousScn;
    @NonNull
    private final String[] schemas;
    private final String[] tables;
    @NonNull
    private final List<Consumer<LogContent>> consumers;
    private Scn fileFirstScn;
    private Scn fileNextScn;
    private Scn recordNextScn;

    public void startLoop(Supplier<Boolean> runningContext,
                          long sleepMillis) {
        while (runningContext.get()) {
            try {
                Thread.sleep(sleepMillis);
                scan();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void scan() throws SQLException {
        if (fileFirstScn == null) {
            fileFirstScn = connection.getFirstScn(previousScn, previousScn);
            fileNextScn = fileFirstScn;
        }

        Optional<LogFile> logFile = connection.getLogFile(fileFirstScn, fileNextScn);
        if (!logFile.isPresent()) {
            return;
        }
        fileNextScn = logFile.get().getNextScn();

        try {
            connection.endLogMiner();
        } catch (SQLException e){
            if (ERROR_CODE_FOR_NOT_ACTIVE_LOGMINER != e.getErrorCode()) {
                throw e;
            }
        }

        connection.addLogFile(logFile.get());

        if (recordNextScn == null) {
            recordNextScn = previousScn;
        }
        connection.startLogMiner(recordNextScn);

        AtomicBoolean hasNextRows = new AtomicBoolean();
        do {
            hasNextRows.set(false);
            connection.readLogContent(recordNextScn, schemas, tables,
                (Consumer<LogContent>) logContent -> {
                    consumers.forEach(c -> c.accept(logContent));

                    recordNextScn = logContent.getScn();
                    hasNextRows.set(true);
                });
        } while (hasNextRows.get());
    }

    @Override
    public void close() {
        try {
            connection.endLogMiner();
        } catch (SQLException e) {
            if (ERROR_CODE_FOR_NOT_ACTIVE_LOGMINER != e.getErrorCode()) {
                throw new RuntimeException(e);
            }
        }
    }
}

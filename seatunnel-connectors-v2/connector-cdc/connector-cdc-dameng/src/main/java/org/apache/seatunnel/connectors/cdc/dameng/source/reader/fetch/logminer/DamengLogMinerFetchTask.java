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

package org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.logminer;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.DamengSourceFetchTaskContext;

import io.debezium.connector.dameng.DamengStreamingChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DamengLogMinerFetchTask implements FetchTask<SourceSplitBase> {

    private final IncrementalSplit incrementalSplit;
    private volatile boolean taskRunning = false;
    @Override
    public void execute(Context context) throws Exception {
        taskRunning = true;

        DamengSourceFetchTaskContext sourceFetchContext = (DamengSourceFetchTaskContext) context;
        DamengStreamingChangeEventSource streamingChangeEventSource = new DamengStreamingChangeEventSource(
            sourceFetchContext.getDbzConnectorConfig(),
            sourceFetchContext.getConnection(),
            incrementalSplit.getTableIds(),
            sourceFetchContext.getDispatcher(),
            sourceFetchContext.getErrorHandler(),
            Clock.SYSTEM,
            sourceFetchContext.getDatabaseSchema());
        DamengLogMinerChangeEventSourceContext changeEventSourceContext =
            new DamengLogMinerChangeEventSourceContext();
        streamingChangeEventSource.execute(changeEventSourceContext, sourceFetchContext.getOffsetContext());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public IncrementalSplit getSplit() {
        return incrementalSplit;
    }

    private class DamengLogMinerChangeEventSourceContext
        implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}

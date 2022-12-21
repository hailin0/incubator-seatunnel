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

package org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.snapshot;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.DamengSourceFetchTaskContext;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DamengSnapshotFetchTask implements FetchTask<SourceSplitBase> {
    private final SnapshotSplit snapshotSplit;
    private volatile boolean taskRunning = false;
    private DamengSnapshotSplitReadTask snapshotSplitReadTask;

    @Override
    public void execute(Context context) throws Exception {
        taskRunning = true;

        DamengSourceFetchTaskContext sourceFetchContext = (DamengSourceFetchTaskContext) context;
        snapshotSplitReadTask = new DamengSnapshotSplitReadTask(
            sourceFetchContext.getDbzConnectorConfig(),
            sourceFetchContext.getOffsetContext(),
            sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
            sourceFetchContext.getDatabaseSchema(),
            sourceFetchContext.getConnection(),
            sourceFetchContext.getDispatcher(),
            snapshotSplit);
        snapshotSplitReadTask.execute(
            new DamengSnapshotSplitChangeEventSourceContext(),
            sourceFetchContext.getOffsetContext());

        taskRunning = false;
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SnapshotSplit getSplit() {
        return snapshotSplit;
    }
}

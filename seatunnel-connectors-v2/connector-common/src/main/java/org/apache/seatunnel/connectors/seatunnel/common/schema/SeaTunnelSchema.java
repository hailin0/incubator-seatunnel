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

package org.apache.seatunnel.connectors.seatunnel.common.schema;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.util.SeaTunnelDataTypeUtils;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import java.io.Serializable;
import java.util.Map;

public class SeaTunnelSchema implements Serializable {

    public static final Option<Schema> SCHEMA =
        Options.key("schema")
            .objectType(Schema.class)
            .noDefaultValue()
            .withDescription("SeaTunnel Schema");
    private static final String FIELD_KEY = "fields";
    private static final String SIMPLE_SCHEMA_FILED = "content";
    private final SeaTunnelRowType seaTunnelRowType;

    private SeaTunnelSchema(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    private static Map<String, String> convertConfigToMap(Config config) {
        // Because the entrySet in typesafe config couldn't keep key-value order
        // So use jackson parsing schema information into a map to keep key-value order
        ConfigRenderOptions options = ConfigRenderOptions.concise();
        String schema = config.root().render(options);
        return SeaTunnelDataTypeUtils.parseDataTypeJsonToSortedMap(schema);
    }

    public static SeaTunnelSchema buildWithConfig(Config schemaConfig) {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(schemaConfig, FIELD_KEY);
        if (!checkResult.isSuccess()) {
            String errorMsg =
                String.format(
                    "Schema config need option [%s], please correct your config first",
                    FIELD_KEY);
            throw new RuntimeException(errorMsg);
        }
        Config fields = schemaConfig.getConfig(FIELD_KEY);
        Map<String, String> fieldsMap = convertConfigToMap(fields);
        return new SeaTunnelSchema(SeaTunnelDataTypeUtils.parseRowType(fieldsMap));
    }

    public static SeaTunnelRowType buildSimpleTextSchema() {
        return new SeaTunnelRowType(
            new String[] {SIMPLE_SCHEMA_FILED},
            new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
    }

    public SeaTunnelRowType getSeaTunnelRowType() {
        return seaTunnelRowType;
    }
}
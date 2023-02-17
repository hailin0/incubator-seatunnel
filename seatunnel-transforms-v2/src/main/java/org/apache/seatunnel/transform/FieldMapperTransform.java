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

package org.apache.seatunnel.transform;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.util.SeaTunnelDataTypeUtils;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.transform.common.AbstractSeaTunnelTransform;
import org.apache.seatunnel.transform.exception.FieldMapperTransformErrorCode;
import org.apache.seatunnel.transform.exception.FieldMapperTransformException;
import org.apache.seatunnel.transform.operator.cast.DataTypeCastOperator;
import org.apache.seatunnel.transform.operator.cast.DataTypeCastOperators;
import org.apache.seatunnel.transform.operator.cast.StringCastOperators;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import com.google.auto.service.AutoService;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@AutoService(SeaTunnelTransform.class)
public class FieldMapperTransform extends AbstractSeaTunnelTransform {
    public static final Option<Map<String, String>> FIELD_MAPPER =
            Options.key("field_mapper")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the field mapping relationship between input and output");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private LinkedHashMap<String, FieldMapperConfig> fieldMapper = new LinkedHashMap<>();
    private List<FieldMapperOperator> fieldMapperOperatorList;

    @Override
    public String getPluginName() {
        return "FieldMapper";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        if (!pluginConfig.hasPath(FIELD_MAPPER.key())) {
            throw new FieldMapperTransformException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "The configuration missing key: " + FIELD_MAPPER);
        }
        this.fieldMapper = convertConfigToSortedMap(pluginConfig.getConfig(FIELD_MAPPER.key()));
    }

    private static LinkedHashMap<String, FieldMapperConfig> convertConfigToSortedMap(Config config) {
        // Because the entrySet in typesafe config couldn't keep key-value order
        // So use jackson parsing schema information into a map to keep key-value order
        ConfigRenderOptions options = ConfigRenderOptions.concise();
        String json = config.root().render(options);
        ObjectNode jsonNodes = JsonUtils.parseObject(json);
        LinkedHashMap<String, FieldMapperConfig> fieldsMapper = new LinkedHashMap<>();
        jsonNodes.fields().forEachRemaining(field -> {
            String fieldKey = field.getKey();
            JsonNode fieldMapperObject = field.getValue();
            try {
                FieldMapperConfig mapperConfig;
                if (fieldMapperObject.isTextual()) {
                    mapperConfig = new FieldMapperConfig(fieldMapperObject.textValue());
                } else {
                    mapperConfig = OBJECT_MAPPER.treeToValue(fieldMapperObject, FieldMapperConfig.class);
                }
                fieldsMapper.put(fieldKey, mapperConfig);
            } catch (Exception e) {
                String errorMsg = String.format("The value [%s] of key [%s] that in config illegal", fieldMapperObject, fieldKey);
                throw new FieldMapperTransformException(CommonErrorCode.ILLEGAL_ARGUMENT, errorMsg);
            }
        });
        return fieldsMapper;
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        List<String> outputFiledNameList = new ArrayList<>(fieldMapper.size());
        List<SeaTunnelDataType<?>> outputDataTypeList = new ArrayList<>(fieldMapper.size());
        List<FieldMapperOperator> fieldMapperOperatorList = new ArrayList<>(fieldMapper.size());
        fieldMapper.forEach((originFieldName, mapperConfig) -> {
            int originFieldIndex = inputRowType.indexOf(originFieldName);
            if (originFieldIndex < 0) {
                throw new FieldMapperTransformException(FieldMapperTransformErrorCode.INPUT_FIELD_NOT_FOUND,
                    "Can not found field " + originFieldName + " from inputRowType");
            }

            SeaTunnelDataType<?> originFieldDataType = inputRowType.getFieldType(originFieldIndex);

            SeaTunnelDataType<?> outputFieldDataType;
            DataTypeCastOperator outputFieldCastOperator;
            if (Strings.isNullOrEmpty(mapperConfig.getOutputDataType())) {
                outputFieldDataType = originFieldDataType;
                outputFieldCastOperator = DataTypeCastOperators.DEFAULT_OPERATOR;
            } else {
                outputFieldDataType = SeaTunnelDataTypeUtils.parseDataType(mapperConfig.getOutputDataType());
                if (originFieldDataType.equals(outputFieldDataType)) {
                    outputFieldCastOperator = DataTypeCastOperators.DEFAULT_OPERATOR;
                } else {
                    outputFieldCastOperator = DataTypeCastOperators.get(originFieldDataType, outputFieldDataType);
                }
            }

            Object defaultValue = null;
            if (!Strings.isNullOrEmpty(mapperConfig.getDefaultValue())) {
                defaultValue = StringCastOperators.create(outputFieldDataType)
                    .cast(mapperConfig.getDefaultValue());
            }
            FieldMapperOperator fieldMapperOperator = new FieldMapperOperator(
                originFieldIndex, defaultValue, mapperConfig.getDescription(), outputFieldCastOperator);

            fieldMapperOperatorList.add(fieldMapperOperator);
            outputDataTypeList.add(outputFieldDataType);

            String outputFieldName = mapperConfig.getOutputField();
            if (Strings.isNullOrEmpty(outputFieldName)) {
                outputFieldName = originFieldName;
            }
            outputFiledNameList.add(outputFieldName);
        });

        this.fieldMapperOperatorList = fieldMapperOperatorList;

        return new SeaTunnelRowType(outputFiledNameList.toArray(new String[0]),
            outputDataTypeList.toArray(new SeaTunnelDataType[0]));
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object[] outputFields = new Object[fieldMapperOperatorList.size()];
        for (int i = 0; i < fieldMapperOperatorList.size(); i++) {
            FieldMapperOperator fieldMapperOperator = fieldMapperOperatorList.get(i);
            Object originField = inputRow.getField(fieldMapperOperator.getFieldIndex());
            if (originField == null) {
                outputFields[i] = fieldMapperOperator.getDefaultValue();
            } else {
                outputFields[i] = fieldMapperOperator.getCastOperator().cast(originField);
            }
        }
        return new SeaTunnelRow(outputFields);
    }

    @Getter
    @NoArgsConstructor
    private static class FieldMapperConfig implements Serializable {
        @NonNull
        @JsonProperty("output_field")
        private String outputField;
        @JsonProperty("output_datatype")
        private String outputDataType;
        @JsonProperty("default_value")
        private String defaultValue;
        @JsonProperty("description")
        private String description;

        public FieldMapperConfig(String outputField) {
            this.outputField = outputField;
        }
    }

    @Getter
    @AllArgsConstructor
    private static class FieldMapperOperator implements Serializable {
        private final int fieldIndex;
        private final Object defaultValue;
        private final String description;
        private final DataTypeCastOperator castOperator;
    }
}

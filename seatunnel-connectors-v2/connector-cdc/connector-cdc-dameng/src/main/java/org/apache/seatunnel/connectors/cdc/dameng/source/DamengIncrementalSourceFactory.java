package org.apache.seatunnel.connectors.cdc.dameng.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class DamengIncrementalSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return DamengIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.BASE_RULE
            .required(
                JdbcSourceOptions.HOSTNAME,
                JdbcSourceOptions.USERNAME,
                JdbcSourceOptions.PASSWORD,
                JdbcSourceOptions.DATABASE_NAME,
                JdbcSourceOptions.TABLE_NAME,
                JdbcCatalogOptions.BASE_URL)
            .optional(
                JdbcSourceOptions.PORT,
                JdbcSourceOptions.SERVER_TIME_ZONE,
                JdbcSourceOptions.CONNECT_TIMEOUT_MS,
                JdbcSourceOptions.CONNECT_MAX_RETRIES,
                JdbcSourceOptions.CONNECTION_POOL_SIZE)
            .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return DamengIncrementalSource.class;
    }
}

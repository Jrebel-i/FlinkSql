package com.hc.redis.v2.source;

import com.hc.redis.mapper.LookupRedisMapper;
import com.hc.redis.options.RedisLookupOptions;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import static com.hc.redis.options.RedisOptions.createValueFormatProjection;


public class RedisDynamicTableSource implements LookupTableSource {

    /**
     * Data type to configure the formats.
     */
    protected final DataType physicalDataType;

    /**
     * Optional format for decoding keys from Kafka.
     */
    protected final @Nullable
    DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    protected final RedisLookupOptions redisLookupOptions;

    private final boolean isDimBatchMode;

    public RedisDynamicTableSource(
            DataType physicalDataType
            , DecodingFormat<DeserializationSchema<RowData>> decodingFormat
            , RedisLookupOptions redisLookupOptions
            , boolean isDimBatchMode) {

        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.decodingFormat = decodingFormat;
        this.redisLookupOptions = redisLookupOptions;

        this.isDimBatchMode = isDimBatchMode;
    }


    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {

        FlinkJedisConfigBase flinkJedisConfigBase = new FlinkJedisPoolConfig.Builder()
                .setHost(this.redisLookupOptions.getHostname())
                .setPort(this.redisLookupOptions.getPort())
                .build();

        LookupRedisMapper lookupRedisMapper = new LookupRedisMapper(
                this.createDeserialization(context, this.decodingFormat, createValueFormatProjection(this.physicalDataType)));

        if (isDimBatchMode) { //开启批量访问外部
            return TableFunctionProvider.of(new RedisRowDataBatchLookupFunction(
                    flinkJedisConfigBase
                    , lookupRedisMapper
                    , this.redisLookupOptions));
//            return TableFunctionProvider.of(new RedisRowDataLookupFunction(
//                    flinkJedisConfigBase
//                    , lookupRedisMapper
//                    , this.redisLookupOptions));
        } else {
            return TableFunctionProvider.of(new RedisRowDataLookupFunction(
                    flinkJedisConfigBase
                    , lookupRedisMapper
                    , this.redisLookupOptions));
        }
    }

    private @Nullable
    DeserializationSchema<RowData> createDeserialization(
            Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.physicalDataType, projection);
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}

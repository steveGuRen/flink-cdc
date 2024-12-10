package org.apache.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class FlinkCDCDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000); // 启用检查点

        // 2. 创建 MySQL CDC Source
        MySqlSource<Row> mySqlSource = MySqlSource.<Row>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test") // 监听的数据库
                .tableList("test.*") // 监听的表
                .username("root")
                .password("abcd1234")
                .deserializer(new MyRowDeserializer()) // 自定义反序列化器
                .startupOptions(StartupOptions.initial()) // 从初始位置开始读取
                .serverTimeZone("UTC")
                .build();

        // 3. 使用 Source 从 MySQL 中读取数据
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .print();

        env.execute("Flink CDC 3.x Demo");
    }
}

class MyRowDeserializer implements DebeziumDeserializationSchema<Row> {

    @Override
    public void deserialize(org.apache.kafka.connect.source.SourceRecord record, Collector<Row> out) throws Exception {
        out.collect(Row.of(record.value().toString()));
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return Types.ROW(Types.STRING);
    }
}
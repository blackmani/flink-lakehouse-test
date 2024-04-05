package com.fan.lakehouse.write;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/*
CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/Users/fanzhe/Downloads/paimon'
);
CREATE TABLE sink_paimon_table (
    name STRING PRIMARY KEY NOT ENFORCED,
    age BIGINT
);
 */
public class WriteToTable {

    public static void writeTo() {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        // env.enableCheckpointing(60_000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a changelog DataStream
        DataStream<Row> dataStream =
                env.fromElements(
                                Row.ofKind(RowKind.INSERT, "Alice", 12),
                                Row.ofKind(RowKind.INSERT, "Bob", 5),
                                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100))
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"name", "age"},
                                        Types.STRING, Types.INT));

        // interpret the DataStream as a Table
        Schema schema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .build();
        Table table = tableEnv.fromChangelogStream(dataStream, schema);

        // create paimon catalog
        tableEnv.executeSql("CREATE CATALOG my_catalog WITH ('type' = 'paimon', 'warehouse'='file:/Users/fanzhe/Downloads/paimon')");
        tableEnv.executeSql("USE CATALOG my_catalog");

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable", table);

        // insert into paimon table from your data stream table
        tableEnv.executeSql("INSERT INTO sink_paimon_table SELECT * FROM InputTable");
    }
}


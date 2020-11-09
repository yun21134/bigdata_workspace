/*
package com.jn.env_streaming;

import com.amazonaws.services.logs.model.DataAlreadyAcceptedException;
import com.cloudera.oryx.lambda.AbstractSparkLayer;
import com.cloudera.oryx.lambda.Functions;
import com.esotericsoftware.kryo.util.ObjectMap;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

public class EnvConsumer {
    private HiveContext hiveContext;
    private JavaStreamingContext jsc;

    private JavaInputDStream<MessageAndMetadata<String, String>> stream;
    private Set<String> topics;
    private Map<String, String> tableNameMap;
    private Map<String, DataFrame> schemaMap;
    private Map<TopicAndPartition, Long> topicAndPartitionMap;
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HHmmss");

    Connection conn;
    private String groupId = "streaming_jn_group_test";


    public static void main(String[] args) throws SQLException,
            ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("user.name", "hdfs");
        EnvConsumer consumer = new EnvConsumer();
        consumer.init();
        consumer.run();
    }

    private void init() throws SQLException, ClassNotFoundException {
        SparkConf sparkConf = new SparkConf()
                .set("spark.streaming.backpressure.enabled", "true")
                .set("spark.streaming.stopGracefullyOnShutdown", "true")
                .set("spark.streaming.backpressure.initialRate", "1000")
                .set("spark.streaming.kafka.maxRatePerPartition", "1000")
                .set("hive.exec.dynamic.partition.mode", "nonstrict")
                .set("hive.metastore.uris", "thrift://nn1:9083," +
                        "thrift://nn2:9083")
                .set("hive.metastore.warehouse.dir", "/user/hive/warehouse")
                .set("hive.exec.dynamic.partition", "true")
                .set("hive.exec.dynamic.partition.mode", "nonstrict")
                .set("spark.port.maxRetries", "128")
                .setAppName("EnvConsumer")
                .setMaster("local[*]");


        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        jsc = new JavaStreamingContext(sparkContext, Durations.seconds(5));
        hiveContext = new HiveContext(sparkContext);

        Map<String, String> kafkaParams = new HashMap<String, String>() {
            {
                put("group.id", groupId);
                //put("zookeeper", "dn1:2181,dn2:2181,dn3:2181");
                put("metadata.broker.list", "dn1:9092,dn2:9092,dn3:9092");
                put("auto.offset.reset", "smallest");
                put("enable.auto.commit", "false");

            }
        };

        topics = new HashSet<String>(Arrays.asList("env_jnxzbmjk_test", "env_jcdwjcxxjk_test"));


        StringDecoder keyDecoder =
                new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder =
                new StringDecoder(new VerifiableProperties());

        // initial table name map -> topic:tableName
        initialTableNameMap();
        // initial schema map -> topic:schema
        createSchemas(sparkContext, hiveContext);
        // initial database connection
        conn = getConn();
        topicAndPartitionMap = getTopicAndPartitionMap();
        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;


        stream = KafkaUtils.createDirectStream(jsc,
                String.class,
                String.class, keyDecoder.getClass(),
                valueDecoder.getClass(), streamClass, kafkaParams,
                topicAndPartitionMap,
                Functions.<MessageAndMetadata<String, String>>identity()
        );


    }

    private void run() {
        stream.foreachRDD(streamRdd -> {
            if (streamRdd.isEmpty()) {

            } else {

                OffsetRange[] allOffsetRanges =
                        ((HasOffsetRanges) streamRdd.rdd()).offsetRanges();
                Map<String, List<OffsetRange>> offsetsByTopic =
                        Arrays.stream(allOffsetRanges).collect(Collectors.groupingBy(OffsetRange::topic));

                offsetsByTopic.forEach((key, value) -> {
                    OffsetRange[] offsetRanges = new OffsetRange[value.size()];
                    value.toArray(offsetRanges);


                    JavaRDD<String> rddContent =
                            streamRdd.filter(streamrecord -> {
                                try {
                                    if (streamrecord.topic().equalsIgnoreCase(key)) {
                                        return true;
                                    } else {
                                        return false;
                                    }
                                } catch (Exception e) {
                                    return false;
                                }
                            }).mapToPair(new MMDToTuple2Fn<String, String>())
                                    .map(x -> x._2()).map(l -> l.replace("\n", " "));


                    DataFrame dataFrame = hiveContext.jsonRDD(rddContent,
                            schemaMap.get(key).schema());

                    if (dataFrame.count() > 0) {

                        try {
                            Boolean success = write2Hive(key, dataFrame,
                                    tableNameMap.get(key));
                            if (success) {
                                // commit offsets
                                Statement st = conn.createStatement();
                                Arrays.stream(offsetRanges).forEach(x -> {
                                    try {
                                        StringBuilder sb = new StringBuilder();
                                        sb.append("replace into t_offset ")
                                                .append("(topic,groupid," +
                                                        "partitions," +
                                                        "fromoffset," +
                                                        "untiloffset) values " +
                                                        "( '")
                                                .append(x.topic() + "', '")
                                                .append(groupId + "', ")
                                                .append(x.partition() + ", ")
                                                .append(x.fromOffset() + ", ")
                                                .append(x.untilOffset() + ") ");

                                        ResultSet rs =
                                                st.executeQuery(sb.toString());

                                        rs.close();
                                    } catch (Exception e) {

                                    }
                                });
                                st.close();
                            } else {
                                rddContent.saveAsTextFile("/user" +
                                        "/hive/baddata/env/" + key + "_" +
                                        String.format(df.format(new Date())));
                            }
                        } catch (Exception e) {

                        }
                    }
                });
            }
        });
        jsc.start();
        jsc.awaitTermination();
    }

    private void initialTableNameMap() {
        tableNameMap = new HashMap<String, String>();

        //1.env_jnxzbmjk,env_jcdwjcxxjk,env_gqxkqzljk,csgz_hjcgqfylb,csgz_clcrjbxxlb
        //1.topic:env_jnxzbmjk table: env_jnxzbmjk
        tableNameMap.put("env_jnxzbmjk_test", "stg.env_jnxzbmjk_test");
        //2.topic:env_jcdwjcxxjk table: env_jcdwjcxxjk
        tableNameMap.put("env_jcdwjcxxjk_test", "stg.env_jcdwjcxxjk_test");
        //3.topic:env_gqxkqzljk table: env_gqxkqzljk
        */
/*tableNameMap.put("env_gqxkqzljk", "stg.env_gqxkqzljk_test");
        //4.topic:csgz_hjcgqfylb table: csgz_hjcgqfylb
        tableNameMap.put("csgz_hjcgqfylb", "stg.csgz_hjcgqfylb_test");
        //5.topic:csgz_clcrjbxxlb table: csgz_clcrjbxxlb
        tableNameMap.put("csgz_clcrjbxxlb", "stg.csgz_clcrjbxxlb_test");*//*


    }

    private void createSchemas(JavaSparkContext spark, HiveContext hive) {

        schemaMap = new HashMap<String, DataFrame>();

        // json schema files path in hdfs: /user/hive/schemas/pro_environmental
        //1.topic:env_jnxzbmjk table: env_jnxzbmjk
        JavaRDD<String> bdcqhyRDD = spark.wholeTextFiles("/user/hive/schemas/pro_environmental/env_jnxzbmjk.json")
                .values().map(x -> x.replace("\n", " "));
        DataFrame bdcqhyDF = hive.jsonRDD(bdcqhyRDD).toSchemaRDD();
        schemaMap.put("env_jnxzbmjk_test", bdcqhyDF);

        //2.topic:env_jcdwjcxxjk table: env_jcdwjcxxjk
        JavaRDD<String> hydjRDD = spark.wholeTextFiles("/user/hive/schemas/pro_environmental/env_jcdwjcxxjk.json")
                .values().map(x -> x.replace("\n", " "));
        DataFrame hydjDF = hive.jsonRDD(hydjRDD).toSchemaRDD();
        schemaMap.put("env_jcdwjcxxjk_test", hydjDF);

        //3.topic:env_gqxkqzljk table: env_gqxkqzljk
        */
/*JavaRDD<String> gsqydjRDD = spark.wholeTextFiles("/user/hive/schemas/pro_environmental/env_gqxkqzljk.json")
                .values().map(x -> x.replace("\n", " "));
        DataFrame gsqydjDF = hive.jsonRDD(gsqydjRDD).toSchemaRDD();
        schemaMap.put("env_gqxkqzljk", gsqydjDF);

        //4.topic:csgz_hjcgqfylb table: csgz_hjcgqfylb
        JavaRDD<String> sfzRDD = spark.wholeTextFiles("/user/hive/schemas/pro_environmental/csgz_hjcgqfylb.json")
                .values().map(x -> x.replace("\n", " "));
        DataFrame sfzDF = hive.jsonRDD(sfzRDD).toSchemaRDD();
        schemaMap.put("csgz_hjcgqfylb", sfzDF);

        //5.topic:csgz_clcrjbxxlb table: csgz_clcrjbxxlb
        JavaRDD<String> jzzRDD = spark.wholeTextFiles("/user/hive/schemas/pro_environmental/csgz_clcrjbxxlb.json")
                .values().map(x -> x.replace("\n", " "));
        DataFrame jzzDF = hive.jsonRDD(jzzRDD).toSchemaRDD();
        schemaMap.put("csgz_clcrjbxxlb", jzzDF);*//*



    }

    private Boolean write2Hive(String topic, DataFrame data, String table) {
        SimpleDateFormat partitionDF = new SimpleDateFormat("yyyyMMdd");//设置日期格式

        try {
            if (data.count() > 0) {
                String partition =
                        String.format(partitionDF.format(new Date()));
                data.registerTempTable("env_temp");
                String sql = " insert into " + table + " partition( " +
                        "date = '" + partition + "') select * from env_temp";

                try {
                    hiveContext.sql(sql);
                } catch (Exception e) {

                    return false;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private Map<TopicAndPartition, Long> getTopicAndPartitionMap()
            throws SQLException, ClassNotFoundException {

        Map<TopicAndPartition, Long> topicAndPartitionMapMap =
                new HashMap<>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from t_offset_test where " +
                "groupid='" + groupId + "' ");
        try {
            while (rs.next()) {
                topicAndPartitionMapMap.put(new TopicAndPartition(rs.getString(1)
                        , rs.getInt(3)), rs.getLong(5));
            }
        } catch (Exception e) {
            System.out.println("get offset from db error: " + e.getMessage());
        } finally {
            rs.close();
            st.close();
        }

        return topicAndPartitionMapMap;
    }

    private static final class MMDToTuple2Fn<K, M> implements PairFunction<MessageAndMetadata<K, M>, K, M> {
        @Override
        public Tuple2<K, M> call(MessageAndMetadata<K, M> km) {
            return new Tuple2<K, M>(km.key(), km.message());
        }
    }

    private Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager.getConnection("jdbc:mysql://172.20.5.176:3306" +
                        "/bigdata_offset?characterEncoding=utf-8",
                "root",
                "MySQL_0430#");
    }
}
*/

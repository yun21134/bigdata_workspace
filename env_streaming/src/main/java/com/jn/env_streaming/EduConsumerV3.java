package com.jn.env_streaming;

import com.cloudera.oryx.lambda.Functions;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.stream.Collectors;

public class EduConsumerV3 {
    private HiveContext hiveContext;
    private JavaStreamingContext jsc;
    private JavaInputDStream<MessageAndMetadata<String, String>> stream;
    //    private Set<String> topics;
    private Map<String, String> tableNameMap;
    private Map<String, DataFrame> schemaMap;
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HHmmss");
    private final String GROUP_ID = "env_streaming_test";

    Connection conn;


    public static void main(String[] args) throws SQLException,
            ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("user.name", "hdfs");
        EduConsumerV3 consumer = new EduConsumerV3();
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
                .setAppName("edu_streaming_spark_jn_v2")
                .setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        jsc = new JavaStreamingContext(sparkContext, Durations.seconds(5));
        hiveContext = new HiveContext(sparkContext);

        Map<String, String> kafkaParams = new HashMap<String, String>() {
            {
                put("group.id", GROUP_ID);
                //put("zookeeper", "dn1:2181,dn2:2181,dn3:2181");
                put("metadata.broker.list", "dn1:9092,dn2:9092,dn3:9092");
                put("auto.offset.reset", "smallest");
                put("enable.auto.commit", "false");
            }
        };

        StringDecoder keyDecoder =
                new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder =
                new StringDecoder(new VerifiableProperties());

        // initial table name map -> topic:tableName
        initialTableNameMap();
        // initial schema map -> topic:schemaRDD
        createSchemas(sparkContext, hiveContext);
        // initial database connection
        conn = getConn();

        Map<TopicAndPartition, Long> topicAndPartitionMap =
                getTopicAndPartitionMap();
        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;

        stream = KafkaUtils.createDirectStream(jsc,
                String.class,
                String.class,
                keyDecoder.getClass(),
                valueDecoder.getClass(),
                streamClass,
                kafkaParams,
                topicAndPartitionMap,
                Functions.<MessageAndMetadata<String, String>>identity()
        );
    }

    private void run() {
        stream.foreachRDD(streamRdd -> {
            if (streamRdd.isEmpty()) {
                // pass empty stream rdd
            } else {
                OffsetRange[] allOffsetRanges =
                        ((HasOffsetRanges) streamRdd.rdd()).offsetRanges();
                Map<String, List<OffsetRange>> offsetsByTopic =
                        Arrays.stream(allOffsetRanges).collect(Collectors.groupingBy(OffsetRange::topic));

                offsetsByTopic.forEach((key, value) -> {
                    OffsetRange[] offsetRanges =
                            value.toArray(new OffsetRange[0]);

                    JavaRDD<String> rddContent =
                            streamRdd.filter(streamrecord -> {
                                try {
                                    return streamrecord.topic().equalsIgnoreCase(key);
                                } catch (Exception e) {
                                    return false;
                                }
                            }).mapToPair(new MMDToTuple2Fn<String, String>())
                                    .map(x -> x._2()).map(l -> l.replace("\n"
                                    , " "));

                    DataFrame dataFrame = hiveContext.jsonRDD(rddContent,
                            schemaMap.get(key).schema());

                    if (dataFrame.count() > 0) {
                        try {
                            Boolean success = write2Hive(dataFrame,
                                    tableNameMap.get(key));
                            if (success) {
                                // commit offsets
                                commitOffsets(offsetRanges);
                            } else {
                                // save issue data
                                saveIssueData(rddContent, key);
                            }
                        } catch (Exception e) {
                            // 写入 hive 失败
                            // 如不处理，下次轮询会重新处理本批次数据
                            // 问题：如果本批次数据有问题，可能会导致消费一直卡在本批次
                            // 解决方式：在此写入错误文件，并更新 offset
                            commitOffsets(offsetRanges);
                            saveIssueData(rddContent, key);
                        }
                    }
                });
            }
        });
        jsc.start();
        jsc.awaitTermination();
    }

    private void initialTableNameMap() {
        tableNameMap = new HashMap<>();

        tableNameMap.put("env_jnxzbmjk","stg.env_jnxzbmjk_test");
        //2.topic:env_jcdwjcxxjk table: env_jcdwjcxxjk
        tableNameMap.put("env_jcdwjcxxjk","stg.env_jcdwjcxxjk_test");
        //3.topic:env_gqxkqzljk table: env_gqxkqzljk
        tableNameMap.put("env_gqxkqzljk","stg.env_gqxkqzljk_test");
        //4.topic:csgz_hjcgqfylb table: csgz_hjcgqfylb
        tableNameMap.put("csgz_hjcgqfylb","stg.csgz_hjcgqfylb_test");
        //5.topic:csgz_clcrjbxxlb table: csgz_clcrjbxxlb
        tableNameMap.put("csgz_clcrjbxxlb","stg.csgz_clcrjbxxlb_test");
    }

    private void createSchemas(JavaSparkContext spark, HiveContext hive) {
        //ClassLoader loader = this.getClass().getClassLoader();
        schemaMap = new HashMap<>();

        // json schema files path in hdfs: /user/hive/schemas/edu
        // 2. 不动产权证书证载信息核验查询 jns_bdcdjzszzxxhx
        // topic: edu_bdcqhy, table: stg.edu_bdcqhy
        JavaRDD<String> env_jnxzbmjkRDD = spark.wholeTextFiles("/user/hive/schemas/pro_environmental/env_jnxzbmjk.json")
                .values().map(x -> x.replace("\n", " "));
        DataFrame env_jnxzbmjkDF = hive.jsonRDD(env_jnxzbmjkRDD).toSchemaRDD();
        schemaMap.put("env_jnxzbmjk", env_jnxzbmjkDF);

        // 3. 省民政厅_婚姻登记信息（双人）mzt_hydjxx1
        // topic: edu_hydj, table: stg.edu_hydj
        JavaRDD<String> env_jcdwjcxxjkRDD = spark.wholeTextFiles("/user/hive/schemas/pro_environmental/env_jcdwjcxxjk.json")
                .values().map(x -> x.replace("\n", " "));
        DataFrame env_jcdwjcxxjkDF = hive.jsonRDD(env_jcdwjcxxjkRDD).toSchemaRDD();
        schemaMap.put("env_jcdwjcxxjk", env_jcdwjcxxjkDF);

        // 4. 省工商企业登记注册信息 shgsh_qyzcdjxx
        // topic: edu_gsqydj, table: stg.edu_shgsh_qyzcdjxx
        JavaRDD<String> env_gqxkqzljkRDD = spark.wholeTextFiles("/user/hive/schemas/pro_environmental/env_gqxkqzljk.json")
                .values().map(x -> x.replace("\n", " "));
        DataFrame env_gqxkqzljkDF = hive.jsonRDD(env_gqxkqzljkRDD).toSchemaRDD();
        schemaMap.put("env_gqxkqzljk", env_gqxkqzljkDF);

        // 5. 身份证信息查询 sfzxxcx
        // topic: edu_sfz, table: stg.edu_sfz
        JavaRDD<String> csgz_hjcgqfylbRDD = spark.wholeTextFiles("/user/hive/schemas/pro_environmental/csgz_hjcgqfylb.json")
                .values().map(x -> x.replace("\n", " "));
        DataFrame csgz_hjcgqfylbDF = hive.jsonRDD(csgz_hjcgqfylbRDD).toSchemaRDD();
        schemaMap.put("csgz_hjcgqfylb", csgz_hjcgqfylbDF);

        // 6. 居住证人员信息查询接口 gaj_jzrxxjk
        // topic: edu_jzz, table: stg.edu_jzz
        JavaRDD<String> csgz_clcrjbxxlbRDD = spark.wholeTextFiles("/user/hive/schemas/pro_environmental/csgz_clcrjbxxlb.json")
                .values().map(x -> x.replace("\n", " "));
        DataFrame csgz_clcrjbxxlbDF = hive.jsonRDD(csgz_clcrjbxxlbRDD).toSchemaRDD();
        schemaMap.put("csgz_clcrjbxxlb", csgz_clcrjbxxlbDF);

    }

    private Boolean write2Hive(DataFrame data, String table) {
        SimpleDateFormat partitionDF = new SimpleDateFormat("yyyyMMdd");//设置日期格式

        try {
            if (data.count() > 0) {
                String partition = partitionDF.format(new Date());
                data.registerTempTable("edu_temp");
                String sql = " insert into " + table + " partition( " +
                        "date = '" + partition + "') select * from edu_temp";
//                String sql = "select * from edu_temp";
                try {
                    hiveContext.sql(sql);
                    return true;
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
            throws SQLException {

        Map<TopicAndPartition, Long> topicAndPartitionMapMap =
                new HashMap<>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from t_offset where " +
                "groupid='" + GROUP_ID + "' ");
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

    private void commitOffsets(OffsetRange[] offsetRanges) {
        try {
            Statement st = conn.createStatement();
            Arrays.stream(offsetRanges).forEach(x -> {
                try {
                    StringBuilder sb = new StringBuilder();
                    sb.append("replace into t_offset ")
                            .append("(topic,groupid,")
                            .append("partitions,")
                            .append("fromoffset,")
                            .append("untiloffset) ")
                            .append(" values ( '")
                            .append(x.topic() + "', '")
                            .append(GROUP_ID + "', ")
                            .append(x.partition() + ", ")
                            .append(x.fromOffset() + ", ")
                            .append(x.untilOffset() + ") ");
                    ResultSet rs =
                            st.executeQuery(sb.toString());
                    rs.close();
                } catch (Exception e) {
                    // mqsql 提交失败，暂不处理
                    // 问题：如果 hive 写入成功，会导致本批次写入 hive 成功的数据重复消费
                }
            });
            st.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private void saveIssueData(JavaRDD<String> data, String topic) {
        data.saveAsTextFile("/user" +
                "/hive/baddata/env/" + topic + "_" +
                df.format(new Date()));
    }
}

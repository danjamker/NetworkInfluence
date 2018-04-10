package Jobs;

import HBase.Connector;
import Network.Direction;
import Network.HBaseNetwork;
import Network.INetwork;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class LoadAu extends IJob {

    private final String table_name;
    private final String input_file, output_dir;
    private final Configuration config;
    private JavaSparkContext _jsc;
    private int partitions = 500;

    public LoadAu(JavaSparkContext _jsc, final String table_name, final String input_file, String output_dir, final Configuration config){
        this._jsc = _jsc;
        this.config = config;
        this.input_file = input_file;
        this.table_name = table_name;
        this.output_dir = output_dir;
    }

    @Override
    public void invoke() throws IOException {
        logger.info("Starting A_u");

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> tmp = _jsc.wholeTextFiles(input_file, partitions).flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, String> stringStringTuple2) throws Exception {

                final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

                Collection<Row> rows_tmp = Collections2.transform(Arrays.asList(stringStringTuple2._2().split("\n")), new Function<String, Row>() {
                    @Nullable
                    @Override
                    public Row apply(@Nullable String s) {
                        String[] tmp = s.split("\t");
                        return new Row(tmp[0], tmp[1], formatter.parseDateTime(tmp[2]));
                    }
                });

                List<Row> rows = new ArrayList<Row>(rows_tmp);
                Collections.sort(rows);
                rows = Utils.Tools.firstInstances(rows);

                List<Tuple2<String, Integer>> ir = new ArrayList<Tuple2<String, Integer>>();
                for (Row r : rows) {
                    ir.add(new Tuple2<String, Integer>(r.getUser(), 1));
                }
                return ir.iterator();
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, Tuple2<String, Tuple2<String, Integer>>>() {
            @Override
            public Iterator<Tuple2<String, Tuple2<String, Integer>>> call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "scc-culture-slave3.lancs.ac.uk");

                Connector hb2 = new Connector.HBaseBuilder().setOverwrite(false).setTableName(table_name).setConfig(config).createHBase();
                INetwork _network = new HBaseNetwork(hb2);
                Tuple2<String, Integer> stringIntegerTuple2;
                List<Tuple2<String, Tuple2<String, Integer>>> out = new ArrayList<Tuple2<String, Tuple2<String, Integer>>>();

                while (tuple2Iterator.hasNext()) {
                    stringIntegerTuple2 = tuple2Iterator.next();
                    _network.loadLocalNetwork(stringIntegerTuple2._1);
                    out.add(new Tuple2<String, Tuple2<String, Integer>>(new String(stringIntegerTuple2._1), stringIntegerTuple2));

                    for (String s : _network.getEdges(stringIntegerTuple2._1, Direction.IN)) {
                        out.add(new Tuple2<String, Tuple2<String, Integer>>(s, stringIntegerTuple2));
                    }

                    for (String s : _network.getEdges(stringIntegerTuple2._1, Direction.OUT)) {
                        out.add(new Tuple2<String, Tuple2<String, Integer>>(s, stringIntegerTuple2));
                    }
                }
                hb2.close();

                return out.iterator();
            }
        }).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Integer>>, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, Tuple2<String, Integer>> stringTuple2Tuple2) throws Exception {
                return stringTuple2Tuple2;
            }
        }).groupByKey();

        tmp.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Tuple2<String,Integer>>>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> stringIterableTuple2) throws Exception {
                List<String> s = new ArrayList<String>();
                for (Tuple2<String, Integer> k : stringIterableTuple2._2) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(Metrics.Metrics._A_u.toString());
                    sb.append(",");
                    sb.append(k._1());
                    sb.append(",");
                    sb.append(k._2);
                    s.add(sb.toString());
                }
                return s.iterator();
            }
        }).saveAsTextFile(output_dir);
    }
}

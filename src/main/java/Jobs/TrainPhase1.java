package Jobs;

import HBase.Connector;
import Metrics.HBaseMetrics;
import Metrics.IMetrics;
import Metrics.Metrics;
import Network.Direction;
import Network.HBaseNetwork;
import Network.INetwork;
import Network.NoEdgeException;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.commons.math3.analysis.function.Divide;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.joda.time.Minutes;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;
import scala.Tuple3;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class TrainPhase1 extends IJob  {

    private final String table_name;
    private final String input_file, output_dir;
    private final HBaseConfiguration config;
    private JavaSparkContext _jsc;
    private int partitions = 500;

    public TrainPhase1(JavaSparkContext _jsc, final String table_name, final String input_file, String output_dir, final HBaseConfiguration config){
        this._jsc = _jsc;
        this.config = config;
        this.input_file = input_file;
        this.table_name = table_name;
        this.output_dir = output_dir;
    }

    @Override
    public void invoke() throws IOException {
        JavaPairRDD<Tuple3<Metrics, String, String>, Double> reduced_values = _jsc
                .wholeTextFiles(input_file, partitions)
                .flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, Tuple3<Metrics, String, String>, Double>() {
            @Override
            public Iterable<Tuple2<Tuple3<Metrics, String, String>, Double>> call(Tuple2<String, String> stringStringTuple2) throws IOException, NoEdgeException {

                Connector hb = new Connector.HBaseBuilder().setOverwrite(false).setTableName(table_name).setConfig(config).createHBase();

                IMetrics _metrics = new HBaseMetrics(hb);
                INetwork _network = new HBaseNetwork(hb);

                List<Row> current_table = new ArrayList<Row>();
                Divide div = new Divide();
                final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

                Collection<Row> rows_tmp = Collections2.transform(Lists.fromArray(stringStringTuple2._2().split("\n")), new Function<String, Row>() {
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
                Collections.sort(rows);
                List<String> parents = new ArrayList<String>();
                for (Row u : rows) {

                    logger.info("Line: " + u.getUser());
                    logger.info(current_table.size() + "/" + rows.size() + "/" + u.getWord());

                    _metrics.setU(u.getUser(), false);

                    _network.loadLocalNetwork(u.getUser());
                    logger.info("    Loaded network");

                    parents.clear();

                    for (Row v : current_table) {

                        //The edges for U have been loaded hoever we what to know about the edge v->u
                        if (_network.hasRelationship(u.getUser(), v.getUser(), Direction.IN)) {
                            if (_network.edgeTime(u.getUser(), v.getUser(), Direction.IN).isBefore(u.getTime())) {
                                if (u.getTime().isAfter(v.getTime())) {

                                    _metrics.global_A_v_2_u(u.getUser(), v.getUser(), 1.0);
                                    _metrics.Tau_v_u(u.getUser(), v.getUser(), new Double(Minutes.minutesBetween(v.getTime(), u.getTime()).getMinutes()));
                                    parents.add(v.getUser());
                                }
                            }
                        }

                        _metrics.A_v_and_u(u.getUser(), v.getUser(), 1.0);

                    }

                    for (String v : parents) {
                        _metrics.credit_v_u(u.getUser(), v, div.value(1.0, parents.size()));
                    }

                    current_table.add(u);

                }

                hb.close();
                return _metrics.toList();
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            //Sum the values togeather
            public Double call(Double integer, Double integer2) throws Exception {
                return integer+integer2;
            }
        });

        reduced_values.mapToPair(new PairFunction<Tuple2<Tuple3<Metrics, String, String>, Double>, Tuple2<Metrics, String>, Tuple2<String, Double>>() {
            @Override
            //Remap the values suck that it is <<Row, ColumFamily>,<Colum,value>>
            public Tuple2<Tuple2<Metrics, String>, Tuple2<String, Double>> call(Tuple2<Tuple3<Metrics, String, String>, Double> tuple3IntegerTuple2) throws Exception {
                return new Tuple2<Tuple2<Metrics, String>, Tuple2<String, Double>>(new Tuple2<Metrics, String>(tuple3IntegerTuple2._1._1(), tuple3IntegerTuple2._1._3()), new Tuple2<String, Double>(tuple3IntegerTuple2._1._2(), tuple3IntegerTuple2._2()));
            }
        }).groupByKey().foreachPartition(new VoidFunction<Iterator<Tuple2<Tuple2<Metrics, String>, Iterable<Tuple2<String, Double>>>>>() {
            @Override
            //Input the values into HBase
            public void call(Iterator<Tuple2<Tuple2<Metrics, String>, Iterable<Tuple2<String, Double>>>> tuple2Iterator) throws Exception {

                Connector hb = new Connector.HBaseBuilder().setOverwrite(false).setTableName(table_name).setConfig(config).createHBase();

                Tuple2<Tuple2<Metrics, String>, Iterable<Tuple2<String, Double>>> tmp = null;
                while (tuple2Iterator.hasNext()) {
                    tmp = tuple2Iterator.next();
                    if (tmp._1()._1() == Metrics._Tau_v_u_credit_v_u || tmp._1()._1() == Metrics._credit_u_v || tmp._1()._1() == Metrics._Tau_u_v_credit_u_v || tmp._1()._1() == Metrics._credit_v_u) {
                        Append a = new Append(Bytes.toBytes(tmp._1()._2()));
                        for (Tuple2<String, Double> t : tmp._2) {
                            //TODO generate row
                            a.add(Bytes.toBytes(tmp._1._1.toString()), Bytes.toBytes(t._1()), Bytes.toBytes("[" + t._2().floatValue() + "]"));
                        }

                        try {
                            logger.info("Appending " + tmp._1()._1() + " for row " + tmp._1._2);
                            hb.Append(a);
                        } catch (IOException e) {
                            logger.error(e);
                        }
                    } else {
                        Increment i = new Increment(Bytes.toBytes(tmp._1()._2()));


                        for (Tuple2<String, Double> t : tmp._2) {
                            i.addColumn(Bytes.toBytes(tmp._1._1.toString()), Bytes.toBytes(t._1()), t._2().longValue());
                        }

                        try {
                            logger.info("Incrementing " + tmp._1()._1() + " for row " + tmp._1._2);
                            hb.Increment(i);
                        } catch (IOException e) {
                            logger.error(e);
                        }
                    }
                }

                hb.close();
            }
        });

        reduced_values.mapToPair(new PairFunction<Tuple2<Tuple3<Metrics, String, String>, Double>, Tuple2<Metrics, String>, Tuple2<String, Double>>() {
            @Override
            //Remap the values suck that it is <<Row, ColumFamily>,<Colum,value>>
            public Tuple2<Tuple2<Metrics, String>, Tuple2<String, Double>> call(Tuple2<Tuple3<Metrics, String, String>, Double> tuple3IntegerTuple2) throws Exception {
                return new Tuple2<Tuple2<Metrics, String>, Tuple2<String, Double>>(new Tuple2<Metrics, String>(Utils.Tools.inverse(tuple3IntegerTuple2._1._1()), tuple3IntegerTuple2._1._2()), new Tuple2<String, Double>(tuple3IntegerTuple2._1._3(), tuple3IntegerTuple2._2()));
            }
        }).groupByKey().foreachPartition(new VoidFunction<Iterator<Tuple2<Tuple2<Metrics, String>, Iterable<Tuple2<String, Double>>>>>() {
            @Override
            //Input the values into HBase
            public void call(Iterator<Tuple2<Tuple2<Metrics, String>, Iterable<Tuple2<String, Double>>>> tuple2Iterator) throws Exception {

                Connector hb = new Connector.HBaseBuilder().setOverwrite(false).setTableName(table_name).setConfig(config).createHBase();

                Tuple2<Tuple2<Metrics, String>, Iterable<Tuple2<String, Double>>> tmp = null;
                while (tuple2Iterator.hasNext()) {
                    tmp = tuple2Iterator.next();

                    if (tmp._1()._1() == Metrics._Tau_v_u_credit_v_u || tmp._1()._1() == Metrics._credit_u_v || tmp._1()._1() == Metrics._Tau_u_v_credit_u_v || tmp._1()._1() == Metrics._credit_v_u) {
                        Append a = new Append(Bytes.toBytes(tmp._1()._2()));
                        for (Tuple2<String, Double> t : tmp._2) {
                            //TODO generate row
                            a.add(Bytes.toBytes(tmp._1._1.toString()), Bytes.toBytes(t._1()), Bytes.toBytes("[" + t._2().floatValue() + "]"));
                        }

                        try {
                            logger.info("Appending " + tmp._1()._1() + " for row " + tmp._1._2);
                            hb.Append(a);
                        } catch (IOException e) {
                            logger.error(e);
                        }
                    } else {
                        Increment i = new Increment(Bytes.toBytes(tmp._1()._2()));


                        for (Tuple2<String, Double> t : tmp._2) {
                            //TODO generate row
                            i.addColumn(Bytes.toBytes(tmp._1._1.toString()), Bytes.toBytes(t._1()), t._2().longValue());
                        }

                        try {
                            logger.info("Incrementing " + tmp._1()._1() + " for row " + tmp._1._2);
                            hb.Increment(i);
                        } catch (IOException e) {
                            logger.error(e);
                        }
                    }
                }

                hb.close();
            }
        });

        reduced_values.map(new org.apache.spark.api.java.function.Function<Tuple2<Tuple3<Metrics, String, String>, Double>, String>() {
            @Override
            public String call(Tuple2<Tuple3<Metrics, String, String>, Double> tuple3DoubleTuple2) throws Exception {

                StringBuilder sb = new StringBuilder();
                sb.append(tuple3DoubleTuple2._1._1());
                sb.append(",");
                sb.append(tuple3DoubleTuple2._1._2());
                sb.append(",");
                sb.append(tuple3DoubleTuple2._1._3());
                sb.append(",");
                sb.append(tuple3DoubleTuple2._2);
                return sb.toString();

            }
        }).saveAsTextFile(output_dir);

        reduced_values.unpersist();
    }
}


package Jobs;

import HBase.Connector;
import Network.Direction;
import Network.HBaseNetwork;
import Network.INetwork;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class LoadNetwork extends IJob {

    private final String table_name;
    private final String input_file;
    private final Configuration config;
    private JavaSparkContext _jsc;

    public LoadNetwork(JavaSparkContext _jsc, final String table_name, final String input_file, final Configuration config) {

        this._jsc = _jsc;
        this.input_file = input_file;
        this.table_name = table_name;
        this.config = config;
    }

    public void invoke() {

        JavaPairRDD<String, Tuple2<String, DateTime>> outdegrees = _jsc.textFile(input_file).mapToPair(new PairFunction<String, String, Tuple2<String, DateTime>>() {
            @Override
            public Tuple2<String, Tuple2<String, DateTime>> call(String s) throws Exception {

                String[] values;
                if (s.contains(",")) {
                    values = s.split(",");
                } else {
                    values = s.split("\001");
                }

                DateTimeFormatter formatter;
                if (values[3].length() == _date_shortform.length()) {
                    formatter = DateTimeFormat.forPattern(_date_shortform);
                } else {
                    formatter = DateTimeFormat.forPattern(_date_longform);
                }

                return new Tuple2<String, Tuple2<String, DateTime>>(values[0], new Tuple2<String, DateTime>(values[1], formatter.parseDateTime(values[3])));

            }
        }).persist(StorageLevel.DISK_ONLY());

        outdegrees.groupByKey().foreachPartition(new VoidFunction<Iterator<Tuple2<String, Iterable<Tuple2<String, DateTime>>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Iterable<Tuple2<String, DateTime>>>> tuple2Iterator) throws Exception {

                Connector hb;
                hb = new Connector.HBaseBuilder().setOverwrite(false).setTableName(table_name).setConfig(config).createHBase();

                INetwork _network = new HBaseNetwork(hb);

                Tuple2<String, Iterable<Tuple2<String, DateTime>>> tup;
                Map<String, DateTime> mapdt;
                while (tuple2Iterator.hasNext()) {
                    tup = tuple2Iterator.next();
                    logger.info(tup);
                    _network.loadLocalNetwork(tup._1());

                    mapdt = new HashMap<String, DateTime>();

                    for (Tuple2<String, DateTime> item : tup._2()) {
                        mapdt.put(item._1(), item._2());
                    }


                    _network.putRelationships(mapdt, Direction.OUT);

                }

                hb.close();
            }
        });

        outdegrees.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, DateTime>>, String, Tuple2<String, DateTime>>() {
            @Override
            public Tuple2<String, Tuple2<String, DateTime>> call(Tuple2<String, Tuple2<String, DateTime>> stringTuple2Tuple2) throws Exception {
                return new Tuple2<String, Tuple2<String, DateTime>>(stringTuple2Tuple2._2()._1(), new Tuple2<String, DateTime>(stringTuple2Tuple2._1(), stringTuple2Tuple2._2()._2()));
            }
        }).groupByKey().foreachPartition(new VoidFunction<Iterator<Tuple2<String, Iterable<Tuple2<String, DateTime>>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Iterable<Tuple2<String, DateTime>>>> tuple2Iterator) throws Exception {

                Connector hb = new Connector.HBaseBuilder().setOverwrite(false).setTableName(table_name).setConfig(config).createHBase();
                INetwork _network = new HBaseNetwork(hb);

                Tuple2<String, Iterable<Tuple2<String, DateTime>>> tup;
                Map<String, DateTime> mapdt;
                while (tuple2Iterator.hasNext()) {
                    tup = tuple2Iterator.next();
                    _network.loadLocalNetwork(tup._1());

                    mapdt = new HashMap<String, DateTime>();

                    for (Tuple2<String, DateTime> item : tup._2()) {
                        mapdt.put(item._1(), item._2());
                    }

                    _network.putRelationships(mapdt, Direction.IN);

                }

                hb.close();
            }
        });

        outdegrees.unpersist();
    }

}

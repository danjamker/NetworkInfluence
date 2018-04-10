package Jobs;

import Evaluation.Performed;
import HBase.Connector;
import Metrics.HBaseMetrics;
import Metrics.IMetrics;
import Metrics.MetricType;
import Metrics.ResultsRow;
import Network.Direction;
import Network.HBaseNetwork;
import Network.INetwork;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.commons.math3.analysis.function.Divide;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;
import scala.Tuple3;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class DiscreteTime extends IJob {

    private final String table_name;
    private final String input_file, output_dir;
    private final Configuration config;
    private JavaSparkContext _jsc;
    private int partitions = 500;
    private Boolean time = false;

    public DiscreteTime(JavaSparkContext _jsc, final String table_name, final String input_file, String output_dir, final Configuration config){
        this._jsc = _jsc;
        this.config = config;
        this.input_file = input_file;
        this.table_name = table_name;
        this.output_dir = output_dir;
    }

    @Override
    public void invoke() throws IOException {
        _jsc.wholeTextFiles(input_file, partitions).flatMap(new FlatMapFunction<Tuple2<String, String>, ResultsRow>() {
            @Override
            public Iterator<ResultsRow> call(Tuple2<String, String> stringStringTuple2) throws Exception {

                Connector hb = new Connector.HBaseBuilder().setOverwrite(false).setTableName(table_name).setConfig(config).createHBase();

                IMetrics _metrics = new HBaseMetrics(hb);
                INetwork _network = new HBaseNetwork(hb);

                Map<String, ResultsRow> results_table = new HashMap<String, ResultsRow>();
                Divide div = new Divide();
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

                for (Row r : rows) {

                    if (results_table.containsKey(r.getUser())) {
                        results_table.get(r.getUser()).setPerfored_u(Performed.perfored);
                        results_table.get(r.getUser()).setTime(r.getTime());
                    } else {
                        results_table.put(r.getUser(), new ResultsRow(r.getUser(), Performed.initiator, r.getWord(), r.getTime()));
                        results_table.get(r.getUser()).setTime(r.getTime());
                        results_table.get(r.getUser()).setP_u(MetricType.Bernoulli, 0, r.getTime());
                        results_table.get(r.getUser()).setP_u(MetricType.Jaccard, 0, r.getTime());
                        results_table.get(r.getUser()).setP_u(MetricType.PC_Bernoulli, 0, r.getTime());
                        results_table.get(r.getUser()).setP_u(MetricType.PC_Jaccard, 0, r.getTime());
                    }

                    _network.loadLocalNetwork(r.getUser());
                    for (String user : _network.getRelationships(r.getUser(), r.getTime(), Direction.IN)) {
                        if (!results_table.containsKey(user)) {
                            results_table.put(user, new ResultsRow(user, Performed.never, r.getWord(), r.getTime()));
                            results_table.get(r.getUser()).setP_u(MetricType.Bernoulli, 0, r.getTime());
                            results_table.get(r.getUser()).setP_u(MetricType.Jaccard, 0, r.getTime());
                            results_table.get(r.getUser()).setP_u(MetricType.PC_Bernoulli, 0, r.getTime());
                            results_table.get(r.getUser()).setP_u(MetricType.PC_Jaccard, 0, r.getTime());
                        }
                    }

                }

                int count = 0;
                for (ResultsRow u : results_table.values()) {
                    logger.info(results_table.values().size()+"/"+count);
                    logger.info(u.getUser());
                    logger.info("------");
                    count = count + 1;

                    Map<DateTime, Tuple2<String, EnumMap<MetricType, Double>>> sorted_parents = new TreeMap<DateTime, Tuple2<String, EnumMap<MetricType, Double>>>();

                    _network.loadLocalNetwork(u.getUser());

                    for (ResultsRow v : results_table.values()) {
                        if (v.getPerfored_u() != Performed.never) {
                            if (_network.hasRelationship(u.getUser(), v.getUser(), Direction.IN)) {
                                if (_network.edgeTime(u.getUser(), v.getUser(), Direction.IN).isBefore(v.getTime())) {
                                    sorted_parents.put(v.getTime(), new Tuple2<String, EnumMap<MetricType, Double>>(v.getUser(), new EnumMap<MetricType, Double>(MetricType.class)));
                                }
                            }
                        }
                    }

                    Map<String, Double> tau = new HashMap<String, Double>();
                    //V needs to be loaded
                    _metrics.setU(u.getUser(), true);

                    for (Tuple2<String, EnumMap<MetricType, Double>> v : sorted_parents.values()) {
//                        _metrics.setU(v._1(), true);

                        double A_u = _metrics.get_A_u(v._1());

                        //Bernoulli
                        Double p_v_u = div.value(new Double(_metrics.get_global_A_u_2_v(u.getUser(), v._1())).floatValue(), A_u);

                        //Jaccard
                        Double Jaccard_p_v_u = div.value(new Double(_metrics.get_global_A_u_2_v(u.getUser(), v._1())), new Double(_metrics.get_A_u_or_v(u.getUser(), v._1())));

                        //Partial credit Bernoulli
                        Double pcb_p_v_u = div.value(new Double(_metrics.get_Tau_u_v_credit_u_v(u.getUser(), v._1())), A_u);

                        //Partial Credit Jaccard
                        Double pcj_p_v_u = div.value(new Double(_metrics.get_Tau_u_v_credit_u_v(u.getUser(), v._1())), new Double(_metrics.get_A_u_or_v(u.getUser(), v._1())));

                        v._2().put(MetricType.Bernoulli, p_v_u);
                        v._2().put(MetricType.Jaccard, Jaccard_p_v_u);
                        v._2().put(MetricType.PC_Bernoulli, pcb_p_v_u);
                        v._2().put(MetricType.PC_Jaccard, pcj_p_v_u);

                        tau.put(u.getUser() + ":" + v._1(), new Double(_metrics.get_Tau_u_v(u.getUser(), v._1())));
                    }

                    Map<DateTime, Tuple2<String, EnumMap<MetricType, Double>>> tmp = new TreeMap<DateTime, Tuple2<String, EnumMap<MetricType, Double>>>();

                    //Set the initial capacity of the queue ot 11, this will increase as more elements are added to it.
                    PriorityQueue<Tuple3<DateTime, String, EnumMap<MetricType, Double>>> queueA = new PriorityQueue<Tuple3<DateTime, String, EnumMap<MetricType, Double>>>(11, new Comparator<Tuple3<DateTime, String, EnumMap<MetricType, Double>>>() {
                        @Override
                        public int compare(Tuple3<DateTime, String, EnumMap<MetricType, Double>> o1, Tuple3<DateTime, String, EnumMap<MetricType, Double>> o2) {
                            if(o1._1().isBefore(o2._1())){
                                return -1;
                            }else if (o1._1().isAfter(o2._1())){
                                return 1;
                            }
                            else{
                                return 0;
                            }
                        }
                    });

                    for (DateTime dt : sorted_parents.keySet()) {

                        if (sorted_parents.get(dt)._2().get(MetricType.Bernoulli) > 0.0 ||
                                sorted_parents.get(dt)._2().get(MetricType.Jaccard) > 0.0 ||
                                sorted_parents.get(dt)._2().get(MetricType.PC_Jaccard) > 0.0 ||
                                sorted_parents.get(dt)._2().get(MetricType.PC_Bernoulli) > 0.0) {

                            tmp.put(dt, sorted_parents.get(dt));

                            queueA.add(new Tuple3<DateTime, String, EnumMap<MetricType, Double>>(dt.plusMinutes(tau.get(u.getUser() + ":" + tmp.get(dt)._1()).intValue()), sorted_parents.get(dt)._1(), sorted_parents.get(dt)._2()));

                            EnumMap<MetricType, Double> intermediate = new EnumMap<MetricType, Double>(MetricType.class);

                            for(MetricType mt: MetricType.values()){
                                intermediate.put(mt, new Double(updateProbability(u.getP_u(mt),sorted_parents.get(dt)._2().get(mt).floatValue())));
                            }

                            while(queueA.peek()._1().isBefore(dt)){
                                Tuple3<DateTime, String, EnumMap<MetricType, Double>> tt = queueA.remove();
                                for(MetricType mt: MetricType.values()){
                                    intermediate.put(mt, new Double(updateRemoveProbability(u.getP_u(mt), tt._3().get(mt).floatValue())));
                                }
                            }

                            for (MetricType mt : intermediate.keySet()) {
                                u.setP_u(mt, intermediate.get(mt).floatValue(), dt);
                            }
                        }
                    }
                }

                hb.close();
                return results_table.values().iterator();
            }
        }).flatMapToPair(new PairFlatMapFunction<ResultsRow, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(ResultsRow resultsRow) throws Exception {
                List<Tuple2<String, String>> r = new ArrayList<Tuple2<String, String>>();
                for(MetricType m: MetricType.values()) {
                    if (time == false) {
                        if (resultsRow.getTimes().size() > 0) {
                            DateTime dt = resultsRow.getTimes().get(resultsRow.getTimes().size() - 1);
                            StringBuilder sb = new StringBuilder();
                            sb.append(m.toString());
                            sb.append(",");
                            sb.append(resultsRow.getUser());
                            sb.append(",");
                            sb.append(resultsRow.getValue());
                            sb.append(",");
                            sb.append(resultsRow.getWord());
                            sb.append(",");
                            sb.append(resultsRow.getPerfored_u());
                            sb.append(",");
                            sb.append(fmt.print(resultsRow.getTime()));
                            sb.append(",");

                            //Get the global max
                            sb.append(resultsRow.get_Max_P_u(m));
                            sb.append(",");
                            sb.append(fmt.print(resultsRow.get_Max_P_u_Time(m)));
                            sb.append(",");
                            sb.append(fmt.print(dt));
                            r.add(new Tuple2<String, String>(m.toString(), sb.toString()));
                        }
                    } else {
                        for (DateTime dt : resultsRow.getTimes()) {
                            StringBuilder sb = new StringBuilder();
                            sb.append(m.toString());
                            sb.append(",");
                            sb.append(resultsRow.getUser());
                            sb.append(",");
                            sb.append(resultsRow.getValue());
                            sb.append(",");
                            sb.append(resultsRow.getWord());
                            sb.append(",");
                            sb.append(resultsRow.getPerfored_u());
                            sb.append(",");
                            sb.append(fmt.print(resultsRow.getTime()));
                            sb.append(",");
                            sb.append(resultsRow.getP_u(m, dt));
                            sb.append(",");
                            sb.append(fmt.print(dt));
                            r.add(new Tuple2<String, String>(m.toString(), sb.toString()));
                        }
                    }
                }
                return r.iterator();
            }
        }).map(new org.apache.spark.api.java.function.Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2;
            }
        }).saveAsTextFile(output_dir);

    }

    private static float updateProbability(float p_u, float p_vu) {
        return p_u + ((1 - p_u) * p_vu);
    }

    public static float updateRemoveProbability(float p_u, float p_vu) {
        return (p_u - p_vu)  / (1 - p_vu);
    }

}

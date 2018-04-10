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
import org.joda.time.Minutes;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class ContinuousTime extends IJob  {

    private final String table_name;
    private final String input_file, output_dir;
    private final Configuration config;
    private JavaSparkContext _jsc;
    private int partitions = 500;
    private Boolean time = false;

    public ContinuousTime(JavaSparkContext _jsc, final String table_name, final String input_file, String output_dir, final Configuration config){
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

                //TODO check to see if the user exsists within the network
                for (Row u : rows) {

                    if (results_table.containsKey(u.getUser())) {
                        results_table.get(u.getUser()).setPerfored_u(Performed.perfored);
                        results_table.get(u.getUser()).setTime(u.getTime());
                    } else {
                        results_table.put(u.getUser(), new ResultsRow(u.getUser(), Performed.initiator, u.getWord(), u.getTime()));
                        results_table.get(u.getUser()).setTime(u.getTime());
                        results_table.get(u.getUser()).setP_u(MetricType.Bernoulli, 0, u.getTime());
                        results_table.get(u.getUser()).setP_u(MetricType.Jaccard, 0, u.getTime());
                        results_table.get(u.getUser()).setP_u(MetricType.PC_Bernoulli, 0, u.getTime());
                        results_table.get(u.getUser()).setP_u(MetricType.PC_Jaccard, 0, u.getTime());
                    }

                    _network.loadLocalNetwork(u.getUser());
                    for (String user : _network.getRelationships(u.getUser(), u.getTime(), Direction.IN)) {
                        if (!results_table.containsKey(user)) {
                            results_table.put(user, new ResultsRow(user, Performed.never, u.getWord(), u.getTime()));
                            results_table.get(u.getUser()).setP_u(MetricType.Bernoulli, 0, u.getTime());
                            results_table.get(u.getUser()).setP_u(MetricType.Jaccard, 0, u.getTime());
                            results_table.get(u.getUser()).setP_u(MetricType.PC_Bernoulli, 0, u.getTime());
                            results_table.get(u.getUser()).setP_u(MetricType.PC_Jaccard, 0, u.getTime());
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

                    _metrics.setU(u.getUser(), true);

                    for (Tuple2<String, EnumMap<MetricType, Double>> v : sorted_parents.values()) {
                        //Load each nabour in as we are looking at the inverse from static time

                        double A_u = _metrics.get_A_u(v._1());

                        //Bernoulli
                        Double p_v_u = div.value(new Double(_metrics.get_global_A_u_2_v(u.getUser(), v._1())).floatValue(), A_u);

                        //Jaccard
                        Double Jaccard_p_v_u = div.value(new Double(_metrics.get_global_A_u_2_v(u.getUser(), v._1())), new Double(_metrics.get_A_u_or_v(u.getUser(), v._1())));

                        //Partial credit Bernoulli
                        Double pcb_p_v_u = div.value(new Double(_metrics.get_total_credits_u_v(u.getUser(), v._1())), A_u);

                        //Partial Credit Jaccard
                        Double pcj_p_v_u = div.value(new Double(_metrics.get_total_credits_u_v(u.getUser(), v._1())), new Double(_metrics.get_A_u_or_v(u.getUser(), v._1())));

                        v._2().put(MetricType.Bernoulli, p_v_u);
                        v._2().put(MetricType.Jaccard, Jaccard_p_v_u);
                        v._2().put(MetricType.PC_Bernoulli, pcb_p_v_u);
                        v._2().put(MetricType.PC_Jaccard, pcj_p_v_u);

                        tau.put(u.getUser()+":"+ v._1(), new Double(_metrics.get_Tau_u_v(u.getUser(), v._1())));
                    }

                    Map<DateTime, Tuple2<String, EnumMap<MetricType, Double>>> tmp = new TreeMap<DateTime, Tuple2<String, EnumMap<MetricType, Double>>>();
                    for (DateTime dt : sorted_parents.keySet()) {

                        if (sorted_parents.get(dt)._2().get(MetricType.Bernoulli) > 0.0 ||
                                sorted_parents.get(dt)._2().get(MetricType.Jaccard) > 0.0 ||
                                sorted_parents.get(dt)._2().get(MetricType.PC_Jaccard) > 0.0 ||
                                sorted_parents.get(dt)._2().get(MetricType.PC_Bernoulli) > 0.0) {
                            tmp.put(dt, sorted_parents.get(dt));


                            Map<MetricType, Double> intermediate = new HashMap<MetricType, Double>();
                            for (DateTime dt2 : tmp.keySet()) {

                                int minutes = Minutes.minutesBetween(dt2, dt).getMinutes();
                                double power = div.value(minutes, tau.get(u.getUser() + ":" + tmp.get(dt2)._1()));

                                for (MetricType mt : tmp.get(dt2)._2().keySet()) {
                                    Double t = tmp.get(dt2)._2().get(mt).floatValue() * Math.exp(power);
                                    if (!intermediate.containsKey(mt)) {
                                        intermediate.put(mt, t);
                                    } else {
                                        intermediate.put(mt, new Double(updateProbability(intermediate.get(mt).floatValue(), t.floatValue())));
                                    }
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

}

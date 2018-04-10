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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class StaticTime extends IJob  {

    private final String table_name;
    private final String input_file, output_dir;
    private final HBaseConfiguration config;
    private JavaSparkContext _jsc;
    private int partitions = 500;
    private Boolean time = false;

    public StaticTime(JavaSparkContext _jsc, final String table_name, final String input_file, String output_dir, final HBaseConfiguration config){
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

                int count = 0;
                for (Row v : rows) {
                    logger.info("Line: " + v.getUser());
                    logger.info(count + "/" + rows.size());
                    count = count + 1;
                    _network.loadLocalNetwork(v.getUser());
                    _metrics.setU(v.getUser(), true);

                    if (results_table.containsKey(v.getUser())) {
                        results_table.get(v.getUser()).setPerfored_u(Performed.perfored);
                    } else {
                        results_table.put(v.getUser(), new ResultsRow(v.getUser(), Performed.initiator, v.getWord(), v.getTime()));
                        results_table.get(v.getUser()).setP_u(MetricType.Bernoulli, 0, v.getTime());
                        results_table.get(v.getUser()).setP_u(MetricType.Jaccard, 0, v.getTime());
                        results_table.get(v.getUser()).setP_u(MetricType.PC_Bernoulli, 0, v.getTime());
                        results_table.get(v.getUser()).setP_u(MetricType.PC_Jaccard, 0, v.getTime());
                    }

                    float A_v = new Double(_metrics.get_A_u(v.getUser())).floatValue();

                    for (String u : _network.getRelationships(v.getUser(), v.getTime(), Direction.OUT)) {

                        //Bernoulli
                        Double p_v_u = div.value(new Double(_metrics.get_global_A_v_2_u(u, v.getUser())).floatValue(), A_v);

                        //Jaccard
                        Double Jaccard_p_v_u = div.value(new Double(_metrics.get_global_A_v_2_u(u, v.getUser())), new Double(_metrics.get_A_v_or_u(u, v.getUser())));

                        //Partial credit Bernoulli
                        Double pcb_p_v_u = div.value(new Double(_metrics.get_total_credits_v_u(u, v.getUser())), A_v);

                        //Partial Credit Jaccard
                        Double pcj_p_v_u = div.value(new Double(_metrics.get_total_credits_v_u(u, v.getUser())), new Double(_metrics.get_A_v_or_u(u, v.getUser())));

                        if (results_table.containsKey(u)) {
                            results_table.get(u).setP_u(MetricType.Bernoulli, updateProbability(results_table.get(u).getP_u(MetricType.Bernoulli), p_v_u.floatValue()), v.getTime());
                            results_table.get(u).setP_u(MetricType.Jaccard, updateProbability(results_table.get(u).getP_u(MetricType.Jaccard), Jaccard_p_v_u.floatValue()), v.getTime());
                            results_table.get(u).setP_u(MetricType.PC_Bernoulli, updateProbability(results_table.get(u).getP_u(MetricType.PC_Bernoulli), pcb_p_v_u.floatValue()), v.getTime());
                            results_table.get(u).setP_u(MetricType.PC_Jaccard, updateProbability(results_table.get(u).getP_u(MetricType.PC_Jaccard), pcj_p_v_u.floatValue()), v.getTime());
                        } else {
                            results_table.put(u, new ResultsRow(u, Performed.never, v.getWord(), v.getTime()));
                            results_table.get(u).setP_u(MetricType.Bernoulli, p_v_u.floatValue(), v.getTime());
                            results_table.get(u).setP_u(MetricType.Jaccard, Jaccard_p_v_u.floatValue(), v.getTime());
                            results_table.get(u).setP_u(MetricType.PC_Bernoulli, pcb_p_v_u.floatValue(), v.getTime());
                            results_table.get(u).setP_u(MetricType.PC_Jaccard, pcj_p_v_u.floatValue(), v.getTime());
                        }
                    }

                }

                return results_table.values();
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

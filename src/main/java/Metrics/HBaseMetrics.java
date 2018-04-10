package Metrics;

import HBase.Connector;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Doubles;
import org.apache.commons.math3.analysis.function.Divide;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import scala.Tuple2;
import scala.Tuple3;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Created by danielkershaw on 16/01/2016.
 * This performs hevier caching on as insted of having to commit before everyload of U it
 * uses nested hash map. This means the commit should have to be performed less oftern.
 */
public class HBaseMetrics implements IMetrics {

    final static Logger logger = Logger.getLogger(HBaseMetrics.class);
    Connector hb;

    Map<Tuple2<String, String>, Double> _A_u;
    Map<Tuple2<String, String>, Double> _global_A_v_2_u;
    Map<Tuple2<String, String>, Double> _A_v_2_u;
    Map<Tuple2<String, String>, Double> _credit_v_u;
    Map<Tuple2<String, String>, Double> _A_v_and_u;
    Map<Tuple2<String, String>, Double> _Tau_v_u;
    Map<Tuple2<String, String>, Double> _Tau_v_u_credit_v_u;

    Double _Influence_u = 0.0;
    String u;
    Result cached;
    Get v;

    public HBaseMetrics(Connector _hb) throws IOException {
        hb = _hb;
        init();
    }

    private static List<Tuple2<Tuple3<Metrics, String, String>, Double>> generateList(Map<Tuple2<String, String>, Double> values, Metrics name) {

        List<Tuple2<Tuple3<Metrics, String, String>, Double>> r_list = new ArrayList<Tuple2<Tuple3<Metrics, String, String>, Double>>();

        for (Tuple2<String, String> key : values.keySet()) {
                if (values.get(key) > 0) {
                    r_list.add(new Tuple2<Tuple3<Metrics, String, String>, Double>(new Tuple3<Metrics, String, String>(name, key._1(), key._2()), values.get(key)));
                }
        }

        return r_list;
    }

    public static Tuple2<String, String> key(String u, String v) {
        return new Tuple2<String, String>(u, v);
    }

    private void init() {
        _A_u = new HashMap<Tuple2<String, String>, Double>();
        _A_v_2_u = new HashMap<Tuple2<String, String>, Double>();
        _credit_v_u = new HashMap<Tuple2<String, String>, Double>();
        _Tau_v_u_credit_v_u = new HashMap<Tuple2<String, String>, Double>();
        _A_v_and_u = new HashMap<Tuple2<String, String>, Double>();
        _Tau_v_u = new HashMap<Tuple2<String, String>, Double>();
        _global_A_v_2_u = new HashMap<Tuple2<String, String>, Double>();
        _Influence_u = 0.0;
    }

    public void setU(String u, boolean load) {

        this.u = u;

        if (load) {
            v = new Get(Bytes.toBytes(u));

            try {
                cached = hb.Get(v);
            } catch (Exception e) {
                logger.error(e);
                cached = null;
            }
        }

    }

    @Override
    public double[] get_credis_v_u(String u, String v) {

        String credis_v_u = "";
        try {
            credis_v_u = Bytes.toString(cached.getValue(Bytes.toBytes(Metrics._credit_v_u.toString()), Bytes.toBytes(u)));
        } catch (Exception e) {
            credis_v_u = "";
        }


        if (credis_v_u != null) {
            Collection<Double> credits = Collections2.transform(Arrays.asList(credis_v_u.split("]")), new Function<String, Double>() {
                @Nullable
                @Override
                public Double apply(@Nullable String s) {
                    return Double.valueOf(s.substring(1, s.length()));
                }
            });

            return Doubles.toArray(credits);
        }

        return new double[0];
    }

    @Override
    public float get_total_credits_v_u(String u, String v) {
        double[] numbers = get_credis_v_u(u, v);
        float sum = 0;
        for (double i : numbers) {
            sum += i;
        }
        return sum;
    }

    @Override
    public void A_u(String u) {
        A_u(u, 1.0);
    }

    @Override
    public void A_u(String v, Double value) {
        try {
            Tuple2<String, String> key = key(u, v);
            if (!_A_u.containsKey(key)) {
                _A_u.put(key, 0.0);
            }
            _A_u.put(key, _A_u.get(key) + value);

        } catch (Exception e) {
            logger.error(e);
        }
    }

    @Override
    public float get_A_u(String u) {

        long A_u = 0;
        try {
            A_u = Bytes.toInt(cached.getValue(Bytes.toBytes(Metrics._A_u.toString()), Bytes.toBytes(u)));

        } catch (Exception e) {
            A_u = 0;
        }

        return A_u;
    }

    @Override
    public void A_v_2_u(String u, String v) {
        A_v_2_u(u, v, 1.0);
    }

    @Override
    public float get_global_A_v_2_u(String u, String v) {

        long A_v_2_u = 0;

        try {
            A_v_2_u = Bytes.toLong(cached.getValue(Bytes.toBytes(Metrics._global_A_v_2_u.toString()), Bytes.toBytes(u)));

        } catch (Exception e) {
            A_v_2_u = 0;
        }

        return A_v_2_u;
    }

    @Override
    public void global_A_v_2_u(String u, String v) {
        global_A_v_2_u(u, v, 1.0);
    }

    @Override
    public void A_v_2_u(String u, String v, Double value) {
        try {
            Tuple2<String, String> key = key(u, v);

            if (!_A_v_2_u.containsKey(key)) {
                _A_v_2_u.put(key, 0.0);
            }
            _A_v_2_u.put(key, _A_v_2_u.get(key) + value);

        } catch (Exception e) {
            logger.error(e);
        }
    }

    @Override
    public float get_A_v_2_u(String u, String v) {
        Get g = new Get(Bytes.toBytes(u));
        g.addColumn(Bytes.toBytes(Metrics._A_v_2_u.toString()), Bytes.toBytes(v));
        long A_v_2_u = 0;
        try {
            Result r = hb.Get(g);

            A_v_2_u = Bytes.toLong(r.getValue(Bytes.toBytes(Metrics._A_v_2_u.toString()), Bytes.toBytes(v)));

        } catch (Exception e) {
            logger.error(e);
        }
        if (A_v_2_u > 0) {
            logger.info(A_v_2_u);
        }
        return A_v_2_u;
    }

    @Override
    public void global_A_v_2_u(String u, String v, Double value) {
        try {
            Tuple2<String, String> key = key(u, v);

            if (!_global_A_v_2_u.containsKey(key)) {
                _global_A_v_2_u.put(key, 0.0);
            }
            _global_A_v_2_u.put(key, _global_A_v_2_u.get(key) + value);

        } catch (Exception e) {
            logger.error(e);
        }
    }

    @Override
    public void credit_v_u(String u, String v, Double value) {

        try {
            Tuple2<String, String> key = key(u, v);

            if (!_credit_v_u.containsKey(key)) {
                _credit_v_u.put(key, 0.0);
            }
            _credit_v_u.put(key, _credit_v_u.get(key) + value);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public float get_credit_v_u(String u, String v) {
        Get g = new Get(Bytes.toBytes(u));
        g.addColumn(Bytes.toBytes(Metrics._credit_v_u.toString()), Bytes.toBytes(v));
        long A_v_2_u = 0;
        try {
            Result r = hb.Get(g);

            A_v_2_u = Bytes.toLong(r.getValue(Bytes.toBytes(Metrics._credit_v_u.toString()), Bytes.toBytes(v)));

        } catch (Exception e) {
            logger.error(e);
        }
        if (A_v_2_u > 0) {
            logger.info(A_v_2_u);
        }
        return A_v_2_u;
    }

    @Override
    public void _Tau_v_u_credit_v_u(String u, String v, Double value) {
        try {
            Tuple2<String, String> key = key(u, v);

            if (!_Tau_v_u_credit_v_u.containsKey(key)) {
                _Tau_v_u_credit_v_u.put(key, 0.0);
            }
            _Tau_v_u_credit_v_u.put(key, _Tau_v_u_credit_v_u.get(key) + value);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    @Override
    public double[] get_Tau_v_u_credit_v_u_array(String u, String v) {
        String credis_v_u = "";
        try {
            credis_v_u = Bytes.toString(cached.getValue(Bytes.toBytes(Metrics._Tau_v_u_credit_v_u.toString()), Bytes.toBytes(u)));
        } catch (Exception e) {
            credis_v_u = "";
        }


        if (credis_v_u != null) {
            Collection<Double> credits = Collections2.transform(Arrays.asList(credis_v_u.split("]")), new Function<String, Double>() {
                @Nullable
                @Override
                public Double apply(@Nullable String s) {
                    return Double.valueOf(s.substring(1, s.length()));
                }
            });

            return Doubles.toArray(credits);
        }

        return new double[0];
    }

    @Override
    public float get_Tau_v_u_credit_v_u(String u, String v) {
        double[] numbers = get_Tau_v_u_credit_v_u_array(u, v);
        float sum = 0;
        for (double i : numbers) {
            sum += i;
        }
        return sum;
    }

    @Override
    public void A_v_and_u(String u, String v) {
        A_v_and_u(u, v, 1.0);
    }

    public float get_A_v_or_u(String u, String v) {
        return get_A_u(u) + get_A_u(v) - get_A_v_and_u(u, v);
    }

    public float get_A_v_and_u(String u, String v) {

        long A_v_and_u = 0;
        try {
            A_v_and_u = Bytes.toLong(cached.getValue(Bytes.toBytes(Metrics._A_v_and_u.toString()), Bytes.toBytes(u)));

        } catch (Exception e) {
            A_v_and_u = 0;
        }
        return A_v_and_u;
    }

    @Override
    public void A_v_and_u(String u, String v, Double value) {
        try {
            Tuple2<String, String> key = key(u, v);

            if (!_A_v_and_u.containsKey(key)) {
                _A_v_and_u.put(key, 0.0);
            }
            _A_v_and_u.put(key, _A_v_and_u.get(key) + value);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    @Override
    public void Tau_v_u(String u, String v, Double value) {
        try {

            Tuple2<String, String> key = key(u, v);
            if(value < 0){
                logger.info("error");
            }

            if (!_Tau_v_u.containsKey(key)) {
                _Tau_v_u.put(key, 0.0);
            }
            _Tau_v_u.put(key, _Tau_v_u.get(key) + value);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    @Override
    public long get_Tau_v_u(String u, String v) {
        Divide d = new Divide();

        try {

            long local_Tau_v_u = Bytes.toLong(cached.getValue(Bytes.toBytes(Metrics._Tau_v_u.toString()), Bytes.toBytes(v)));
            long local_A_v_2_u = Bytes.toLong(cached.getValue(Bytes.toBytes(Metrics._global_A_v_2_u.toString()), Bytes.toBytes(v)));

            return new Double(d.value(local_Tau_v_u, local_A_v_2_u)).longValue();
        } catch (Exception e) {
            logger.error(e);
        }
        return 0l;
    }

    @Override
    public void Influence_u(String u, Double value) {
        try {
            _Influence_u = _Influence_u + value;
        } catch (Exception e) {
            logger.error(e);
        }
    }

    @Override
    public long get_Influence_u(String u) {
        Get g = new Get(Bytes.toBytes(u));
        Divide d = new Divide();

        g.addColumn(Bytes.toBytes(Metrics._Influence_u.toString()), Bytes.toBytes(Metrics._Influence_u.toString()));
        g.addColumn(Bytes.toBytes(Metrics._A_u.toString()), Bytes.toBytes(Metrics._A_u.toString()));

        try {
            Result r = hb.Get(g);

            long local_Influence_u = Bytes.toLong(r.getValue(Bytes.toBytes(Metrics._Influence_u.toString()), Bytes.toBytes(Metrics._Influence_u.toString())));
            long local_A_u = Bytes.toLong(r.getValue(Bytes.toBytes(Metrics._A_u.toString()), Bytes.toBytes(Metrics._A_u.toString())));

            return new Double(d.value(local_Influence_u, local_A_u)).longValue();
        } catch (Exception e) {
            logger.error(e);
        }
        return 0l;
    }

    @Override
    public void commit() {

        try {

            Map<String, Increment> imap = new HashMap<String, Increment>();
            Map<String, Append> amap = new HashMap<String, Append>();

            if (!imap.containsKey(u)) {
                imap.put(u, new Increment(Bytes.toBytes(u)));
            }

            imap.get(u).addColumn(Bytes.toBytes(Metrics._Influence_u.toString()), Bytes.toBytes(Metrics._Influence_u.toString()), _Influence_u.longValue());

            for (Tuple2<String, String> key : _A_v_2_u.keySet()) {
                    if (!imap.containsKey(key._2())) {
                        imap.put(key._2(), new Increment(Bytes.toBytes(key._2())));
                    }
                    imap.get(key._2()).addColumn(Bytes.toBytes(Metrics._A_v_2_u.toString()), Bytes.toBytes(u), _A_v_2_u.get(key).longValue());
            }


            for (Tuple2<String, String> key  : _credit_v_u.keySet()) {

                    if (!amap.containsKey(key._2())) {
                        amap.put(key._2(), new Append(Bytes.toBytes(key._2())));
                    }
                    amap.get(key._2()).add(Bytes.toBytes(Metrics._credit_v_u.toString()), Bytes.toBytes(u), Bytes.toBytes("[" + _credit_v_u.get(key).floatValue() + "]"));


            }

            for (Tuple2<String, String> key  : _Tau_v_u_credit_v_u.keySet()) {
                    if (!amap.containsKey(key._2())) {
                        amap.put(key._2(), new Append(Bytes.toBytes(key._2())));
                    }
                    amap.get(key._2()).add(Bytes.toBytes(Metrics._Tau_v_u_credit_v_u.toString()), Bytes.toBytes(u), Bytes.toBytes("[" + _Tau_v_u_credit_v_u.get(key).floatValue() + "]"));

            }

            for (Tuple2<String, String> key  : _A_v_and_u.keySet()) {
                    if (!imap.containsKey(key._2())) {
                        imap.put(key._2(), new Increment(Bytes.toBytes(key._2())));
                    }
                    imap.get(key._2()).addColumn(Bytes.toBytes(Metrics._A_v_and_u.toString()), Bytes.toBytes(u), _A_v_and_u.get(key).longValue());

            }

            for (Tuple2<String, String> key  : _Tau_v_u.keySet()) {
                    if (!imap.containsKey(key._2())) {
                        imap.put(key._2(), new Increment(Bytes.toBytes(key._2())));
                    }
                    imap.get(key._2()).addColumn(Bytes.toBytes(Metrics._Tau_v_u.toString()), Bytes.toBytes(u), _Tau_v_u.get(key).longValue());

            }

            for (Tuple2<String, String> key  : _global_A_v_2_u.keySet()) {
                    if (!imap.containsKey(key._2())) {
                        imap.put(key._2(), new Increment(Bytes.toBytes(key._2())));
                    }
                    imap.get(key._2()).addColumn(Bytes.toBytes(Metrics._global_A_v_2_u.toString()), Bytes.toBytes(u), _global_A_v_2_u.get(key).longValue());

            }

            hb.Batch(new ArrayList<Row>(imap.values()));
            hb.Batch(new ArrayList<Row>(amap.values()));

        } catch (RetriesExhaustedWithDetailsException e) {
            logger.error(e.getExhaustiveDescription());

        } catch (Exception e) {
            logger.error(e);
        }
        init();
    }

    public List<Tuple2<Tuple3<Metrics, String, String>, Double>> toList() {

        List<Tuple2<Tuple3<Metrics, String, String>, Double>> r_list = new ArrayList<Tuple2<Tuple3<Metrics, String, String>, Double>>();
        r_list.addAll(generateList(_A_v_2_u, Metrics._A_v_2_u));
        r_list.addAll(generateList(_credit_v_u, Metrics._credit_v_u));
        r_list.addAll(generateList(_Tau_v_u_credit_v_u, Metrics._Tau_v_u_credit_v_u));
        r_list.addAll(generateList(_A_v_and_u, Metrics._A_v_and_u));
        r_list.addAll(generateList(_Tau_v_u, Metrics._Tau_v_u));
        r_list.addAll(generateList(_global_A_v_2_u, Metrics._global_A_v_2_u));
        r_list.addAll(Utils.Tools.generateList(_Influence_u, u, Metrics._Influence_u));

        return r_list;
    }
}

package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import scala.Tuple3;

import Metrics.Metrics;
/**
 * Created by danielkershaw on 21/03/2016.
 */
public class Tools {

    final static Logger logger = Logger.getLogger(Tools.class);

    public static Metrics inverse(Metrics mt) {
        switch (mt) {
            case _A_u:
                return Metrics._A_v;
            case _A_v:
                return Metrics._A_u;
            case _global_A_v_2_u:
                return Metrics._global_A_u_2_v;
            case _global_A_u_2_v:
                return Metrics._global_A_v_2_u;
            case _A_v_2_u:
                return Metrics._A_u_2_v;
            case _A_u_2_v:
                return Metrics._A_v_2_u;
            case _credit_v_u:
                return Metrics._credit_u_v;
            case _credit_u_v:
                return Metrics._credit_v_u;
            case _A_v_and_u:
                return Metrics._A_u_and_v;
            case _A_u_and_v:
                return Metrics._A_v_and_u;
            case _Tau_v_u:
                return Metrics._Tau_u_v;
            case _Tau_u_v:
                return Metrics._Tau_v_u;
            case _Tau_v_u_credit_v_u:
                return Metrics._Tau_u_v_credit_u_v;
            case _Tau_u_v_credit_u_v:
                return Metrics._Tau_v_u_credit_v_u;
            case _Influence_u:
                return Metrics._Influence_v;
            case _Influence_v:
                return Metrics._Influence_u;
            default:
                return Metrics._A_u;
        }
    }

    public static List<String> filesinFolder(String path) {

        Path pt = new Path(path);
        FileSystem fs = null;

        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://scc-culture-mind.lancs.ac.uk:8020");
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            logger.error("Error loading file from HDFS", e);
        }

        //3. Get the metadata of the desired directory
        FileStatus[] fileStatus = null;
        try {
            fileStatus = fs.listStatus(pt);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<String> paths = new ArrayList<String>();
        for (int i = 0; i < fileStatus.length; i++) {
            paths.add(fileStatus[i].getPath().getName());
        }

        return paths;
    }

    public static List<Tuple2<Tuple3<Metrics, String, String>, Double>> generateList(Map<String, Map<String, Double>> values, Metrics name) {

        List<Tuple2<Tuple3<Metrics, String, String>, Double>> r_list = new ArrayList<Tuple2<Tuple3<Metrics, String, String>, Double>>();

        for (String u : values.keySet()) {
            for (String v : values.get(u).keySet()) {
                if (values.get(u).get(v) > 0) {
                    r_list.add(new Tuple2<Tuple3<Metrics, String, String>, Double>(new Tuple3<Metrics, String, String>(name, u, v), values.get(u).get(v)));
                }
            }
        }

        return r_list;
    }

    public static List<Tuple2<Tuple3<Metrics, String, String>, Double>> generateList(Double value, String u, Metrics name) {

        List<Tuple2<Tuple3<Metrics, String, String>, Double>> r_list = new ArrayList<Tuple2<Tuple3<Metrics, String, String>, Double>>();
        r_list.add(new Tuple2<Tuple3<Metrics, String, String>, Double>(new Tuple3<Metrics, String, String>(name, u, name.toString()), value));

        return r_list;
    }

}

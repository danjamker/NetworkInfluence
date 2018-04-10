import Jobs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Runner {

    public static void main(String[] args) throws Exception {

        String table_name = "";
        String input_file = "";
        String output_file = "";

        SparkConf _sc;
        _sc = new SparkConf()
                .setMaster("local")
                .setAppName("NetworkInfluence");
        final JavaSparkContext _jsc = new JavaSparkContext(_sc);

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "scc-culture-slave3.lancs.ac.uk");

        new SetUpHBase(config, table_name).invoke();

        new LoadNetwork(_jsc, table_name, input_file, config).invoke();

        new LoadAu(_jsc, table_name, input_file, output_file, config).invoke();

        new TrainPhase1(_jsc, table_name, input_file, output_file, config).invoke();

        new TrainPhase2(_jsc, table_name, input_file, output_file, config).invoke();

        new StaticTime(_jsc, table_name, input_file, output_file, config).invoke();

        new ContinuousTime(_jsc, table_name, input_file, output_file, config).invoke();

        new DiscreteTime(_jsc, table_name, input_file, output_file, config).invoke();
    }
}

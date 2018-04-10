import Jobs.LoadNetwork;
import Jobs.SetUpHBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Runner {

    public static void main(String[] args) throws Exception {

        String table_name = "";
        String input_file = "";

        SparkConf _sc;
        _sc = new SparkConf()
                .setMaster("local")
                .setAppName("NetworkInfluence");
        final JavaSparkContext _jsc = new JavaSparkContext(_sc);

        Configuration config = HBaseConfiguration.create();

        new SetUpHBase(config, table_name).invoke();

        new LoadNetwork(_jsc, table_name, input_file, config).invoke();

    }
}

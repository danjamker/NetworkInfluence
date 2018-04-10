package Jobs;

import HBase.Connector;
import Network.Direction;
import Metrics.Metrics;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SetUpHBase extends IJob {

    private final HBaseConfiguration config;
    private String table_name;

    public SetUpHBase(final HBaseConfiguration config, String table_name){
        this.config = config;
        this.table_name = table_name;
    }

    @Override
    public void invoke() throws IOException {
        List<String> cf = new ArrayList<String>();

        for (Metrics m : Metrics.values()) cf.add(m.toString());

        for (Direction d : Direction.values()) {
            cf.add(d.toString());
        }

        Connector hb = new Connector.HBaseBuilder()
                .setOverwrite(true)
                .setTableName(table_name)
                .setColumeFamilies(cf)
                .setConfig(config)
                .createHBase();

        hb.close();
    }
}

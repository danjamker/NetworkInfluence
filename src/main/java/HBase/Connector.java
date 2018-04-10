package HBase;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Row;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

/**
 * Created by kershad1 on 26/10/2015.
 */
public class Connector {

    protected final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(this.getClass());

    protected Boolean overwrite;
    protected String table_name;
    protected List<String> column_families;
    protected Integer number_of_versions;
    protected Integer splits;
    protected HBaseBuilder builder;
    Configuration conf;
    HBaseAdmin admin;
    HTable table;

    private Connector(HBaseBuilder builder) throws IOException {

        this.builder = builder;
        overwrite = builder.overwrite;
        number_of_versions = builder.number_of_versions;
        splits = builder.splits;
        table_name = builder.tablename;
        conf = builder.config;
        column_families = builder.colume_families;

        admin = new HBaseAdmin(conf);

        if (overwrite == true) {
            if (column_families == null || column_families.size() == 0) {
                throw new IOException("To build table you must specify column names");
            }
            {
                buildTable(column_families);
            }
        }

        table = new HTable(conf, TableName.valueOf(table_name));

    }

    public void close() throws IOException {
        this.admin.close();
        this.table.close();
    }

    public void Put(List<Put> o) throws IOException {
        table.put(o);
    }

    public void Put(Put o) throws IOException {
        table.put(o);
    }

    public void Batch(List<? extends Row> r) throws IOException, InterruptedException {
        table.batch(r);
    }
    public void Append(Append a) throws IOException {
        table.append(a);
    }

    public Result Get(Get g) throws IOException {
        return table.get(g);
    }

    public void Increment(Increment o) throws IOException {
        table.increment(o);
    }

    protected void buildTable(@NotNull List<String> colume_families) throws IOException {
        try {

            //Check to see if the table exsists
            if (this.admin.tableExists(this.table_name) == false) {

                //If not build new table
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(this.table_name));

                //Build columns
                for (String cc : this.column_families) {
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(cc);
                    columnDescriptor.setMaxVersions(this.number_of_versions);
                    tableDescriptor.addFamily(columnDescriptor);
                    if (StringUtils.isAllUpperCase(cc)) {
                        columnDescriptor.setInMemory(true);
                    }
                }

                //create table
                this.admin.createTable(tableDescriptor);
            }

        } catch (IOException e) {
            logger.error(e);
            throw e;
        }
    }

    public static class HBaseBuilder {

        private Integer splits = 10;
        private Boolean overwrite = false;
        private Integer number_of_versions = 30;
        private Configuration config = null;
        private String tablename = null;
        private List<String> colume_families;

        public Connector createHBase() throws IOException {

            Validate.notNull(config);
            return new Connector(this);

        }

        public HBaseBuilder setOverwrite(@NotNull Boolean overwrite) {
            this.overwrite = overwrite;
            return this;
        }

        public HBaseBuilder setVersions(@NotNull Integer number) {
            this.number_of_versions = number;
            return this;
        }

        public HBaseBuilder setSplits(@NotNull Integer number) {
            this.splits = number;
            return this;
        }

        public HBaseBuilder setConfig(@NotNull Configuration conf) {
            this.config = conf;
            return this;
        }

        public HBaseBuilder setTableName(@NotNull String name) {
            this.tablename = name;
            return this;
        }

        public HBaseBuilder setColumeFamilies(@NotNull List<String> colume_families) {
            this.colume_families = colume_families;
            return this;
        }

    }

}
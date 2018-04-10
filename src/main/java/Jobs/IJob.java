package Jobs;

import Network.HBaseNetwork;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;

abstract public class IJob {

    final static Logger logger = Logger.getLogger(IJob.class);
    final static String _date_longform = "yyyy-MM-dd HH:mm:ss";
    final static String _date_shortform = "yyyy-MM-dd";
    final static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    abstract public void invoke() throws IOException;

}

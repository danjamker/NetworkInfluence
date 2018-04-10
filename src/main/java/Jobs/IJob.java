package Jobs;

import Network.HBaseNetwork;
import org.apache.log4j.Logger;

import java.io.IOException;

abstract public class IJob {

    final static Logger logger = Logger.getLogger(IJob.class);
    final static String _date_longform = "yyyy-MM-dd HH:mm:ss";
    final static String _date_shortform = "yyyy-MM-dd";

    abstract public void invoke() throws IOException;

}

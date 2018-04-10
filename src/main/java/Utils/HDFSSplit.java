package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by kershad1 on 21/01/2016.
 */
public class HDFSSplit {

    final static Logger logger = Logger.getLogger(HDFSSplit.class);

    public static void main(String[] args) throws Exception {

        String input = "";
        String output = "";
        String HDFSURI = "";
        try {
            trainTestSplit(input, output, HDFSURI);
        }
        catch (Exception e){
            logger.info(e);
        }


    }

    private static void trainTestSplit(String input, String output, String HDFSURI) throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(HDFSURI), configuration);
        FileStatus[] status = fs.listStatus(new Path(input));  // you need to pass in your hdfs path

        List<FileStatus> files = new LinkedList<FileStatus>(Arrays.asList(status));

        List<FileStatus> test = pickNRandom(files, new Double(files.size()*0.2).intValue());

        Collection<FileStatus> filesh = new HashSet<FileStatus>(files);
        Collection<FileStatus> testh = new HashSet<FileStatus>(test);

        filesh.removeAll(testh);

        fs.mkdirs(new Path(output+"/test/"));
        fs.mkdirs(new Path(output+"/train/"));

        //Move to training file
        for (FileStatus file : testh) {
            System.out.println(file.getPath().getName() +" - test");
            fs.rename(file.getPath(), new Path(output+"/test/"+file.getPath().getName()));
        }

        //Move to testing file
        for (FileStatus file : filesh) {
            System.out.println(file.getPath().getName() +" - train");
            fs.rename(file.getPath(), new Path(output+"/train/"+file.getPath().getName()));
        }
    }

    public static <T> List<T> pickNRandom(List<T> lst, int n) {
        List<T> copy = new LinkedList<T>(lst);
        Collections.shuffle(copy);
        return copy.subList(0, n);
    }

    public static void tagsplit(String input, String output, String HDFSURI) throws IOException, URISyntaxException {
        List<String> tmp = splitOnSuffix(input, output, HDFSURI);

        for (String s : tmp) {
            trainTestSplit(s, output + "-split", HDFSURI);
        }
    }

    public static List<String> splitOnSuffix(String input, String output, String HDFSURI) throws IOException, URISyntaxException {

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(HDFSURI), configuration);
        FileStatus[] status = fs.listStatus(new Path(input));  // you need to pass in your hdfs path

        List<String> newdir = new ArrayList<String>();

        for (FileStatus file : status) {
            if (!file.getPath().getName().startsWith("_S")) {
                fs.mkdirs(new Path(output+"/"+file.getPath().getName().split("-")[1]+"/"));
                System.out.println(file.getPath().getName().split("-")[1]);
            }
        }

            //Move to training file
        for (FileStatus file : status) {
            if (!file.getPath().getName().startsWith("_S")) {
                System.out.println(file.getPath().getName() + " - " + output + "/" + file.getPath().getName().split("-")[1] + "/" + file.getPath().getName());
                fs.rename(file.getPath(), new Path(output + "/" + file.getPath().getName().split("-")[1] + "/" + file.getPath().getName()));
            }
        }

        return newdir;

    }

}

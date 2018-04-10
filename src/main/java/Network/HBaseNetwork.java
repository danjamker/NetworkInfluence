package Network;

import HBase.Connector;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.*;

public class HBaseNetwork implements INetwork {

    final static Logger logger = Logger.getLogger(HBaseNetwork.class);
    Connector hb;

    Map<Direction, Map<String, DateTime>> users;
    String us;

    public HBaseNetwork(Connector _hb) {
        users = new EnumMap<Direction, Map<String, DateTime>>(Direction.class);

        for (Direction d : Direction.values()) {
            users.put(d, new HashMap<String, DateTime>());
        }

        hb = _hb;
    }

    @Override
    public void loadLocalNetwork(String u) throws IOException {

        users.clear();
        for (Direction d : Direction.values()) {
            users.put(d, new HashMap<String, DateTime>());
        }

        us = u;

        Get g = new Get(Bytes.toBytes(u));

        for (Direction dir : Direction.values()) {
            g.addFamily(Bytes.toBytes(dir.toString()));
        }

        Result rs = hb.Get(g);
        CellScanner scanner = rs.cellScanner();
        String user;
        String s;
        Direction dir;

        while (scanner.advance()) {
            Cell cell = scanner.current();
            user = new String(CellUtil.cloneQualifier(cell)); //end of the edge
            s = new String(CellUtil.cloneValue(cell)); //Time of the edge
            dir = Direction.valueOf(new String(CellUtil.cloneFamily(cell))); //Direction of the edge
            users.get(dir).put(user, new DateTime(s));
        }

    }

    @Override
    public Boolean hasRelationship(String u, String v, Direction d) {
        return users.get(d).containsKey(v);
    }

    @Override
    public DateTime edgeTime(String u, String v, Direction d) throws NoEdgeException {
        return users.get(d).get(v);
    }

    @Override
    public void putRelationships(Map<String, DateTime> relations, Direction d) throws IOException {

        Put p = new Put(Bytes.toBytes(us));

        for (String k : relations.keySet()) {
            p.add(Bytes.toBytes(d.toString()), Bytes.toBytes(k), Bytes.toBytes(relations.get(k).toString()));
        }

        hb.Put(p);

    }

    @Override
    public List<String> getRelationships(String u, DateTime dt, Direction d) {

        Get g = new Get(Bytes.toBytes(u));
        g.addFamily(Bytes.toBytes(d.toString()));

        List<String> uu = new ArrayList<String>();
        for (String s : users.get(d).keySet()) {
            if (dt.isAfter(users.get(d).get(s))) {
                uu.add(s);
            }
        }
        return uu;
    }

    @Override
    public List<String> getEdges(String u, Direction d) {
        return new ArrayList<String>(users.get(d).keySet());
    }


}

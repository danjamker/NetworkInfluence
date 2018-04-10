package Network;

import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public interface INetwork {

    void loadLocalNetwork(String u) throws IOException;

    Boolean hasRelationship(String u, String v, Direction d);

    DateTime edgeTime(String u, String v, Direction d) throws NoEdgeException;

    void putRelationships(Map<String, DateTime> relations, Direction d) throws IOException;

    List<String> getRelationships(String u, DateTime dt, Direction d);

    List<String> getEdges(String u, Direction d);

}

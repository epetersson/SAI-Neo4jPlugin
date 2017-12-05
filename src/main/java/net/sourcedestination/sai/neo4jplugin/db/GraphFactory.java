package net.sourcedestination.sai.neo4jplugin.db;

import net.sourcedestination.sai.graph.Feature;
import net.sourcedestination.sai.graph.Graph;
import net.sourcedestination.sai.graph.MutableGraph;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.util.Set;
import java.util.stream.Stream;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by ep on 2017-12-01.
 */
public class GraphFactory<G extends Graph> {
    Driver driver;

    public GraphFactory(Driver driver){
        this.driver = driver;
    }

    /**
     * Method retrieves a graph from the Neo4j db and puts the result into a
     * sai graph
     * @param graphId int
     * @param <G> Graph
     * @return Graph
     */
    public <G extends Graph> G retrieveGraph(int graphId) {
        MutableGraph graph = new MutableGraph();
        try (Session session = driver.session())
        {
            // Auto-commit transactions are a quick and easy way to wrap a read.
            StatementResult graphResult = session.run(
                    "MATCH (g:Graph { gid: {graphId} })-[:HAS_FEATURE]->(gf:Feature), (g)-[:HAS_NODE]->(n:Node), " +
                            "(n)-[:HAS_FEATURE]->(nf:Feature), (n)-[:HAS_EDGE]->(e:Edge), (n2:Node {nid: e.nid2}), " +
                            "(e)-[:HAS_FEATURE]->(ef:Feature) " +
                            "return gf.name, gf.value, n.nid, nf.name, nf.value, e.eid, e.nid1, e.nid2, ef.name, ef.value",
                    parameters("graphId", graphId));
            // Each Cypher execution returns a stream of records.
            while (graphResult.hasNext())
            {
                Record record = graphResult.next();
                graph.addFeature(new Feature(record.get("gf.name").asString(), record.get("gf.value").asString()));
                if(!graph.getNodeIDs().anyMatch(id -> id.equals(record.get("n.nid").asInt())))
                    graph.addNode(record.get("n.nid").asInt());

                graph.addNodeFeature(record.get("n.nid").asInt(), new Feature(record.get("nf.name").asString(),
                        record.get("nf.value").asString()));
                if(!graph.getEdgeIDs().anyMatch(id -> id.equals(record.get("e.eid").asInt())))
                    graph.addEdge(record.get("e.eid").asInt(), record.get("e.nid1").asInt(), record.get("e.nid2").asInt());

                graph.addEdgeFeature(record.get("e.eid").asInt(), new Feature(record.get("ef.name").asString(),
                        record.get("ef.value").asString()) );
            }
        }
        return (G) graph;
    }
}

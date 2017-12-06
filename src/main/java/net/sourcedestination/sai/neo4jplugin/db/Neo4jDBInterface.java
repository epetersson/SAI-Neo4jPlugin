package net.sourcedestination.sai.neo4jplugin.db;

import net.sourcedestination.sai.db.DBInterface;
import net.sourcedestination.sai.graph.Feature;
import net.sourcedestination.sai.graph.Graph;
import net.sourcedestination.sai.graph.GraphFactory;
import net.sourcedestination.sai.graph.MutableGraph;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.GraphDatabase;

import org.apache.log4j.Logger;
import java.util.stream.Stream;
import java.io.File;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by ep on 2017-11-27.
 */
public class Neo4jDBInterface implements DBInterface {
    Driver driver;
    private static final Logger logger = Logger.getLogger(Neo4jDBInterface.class);
    private File dbFile;

    private long nextFeatureID = 1;
    private long nextGraphID = 1;

    public Neo4jDBInterface(){
        driver = GraphDatabase.driver("bolt://localhost:7687");
    }

    @Override
    public void disconnect() {
    }

    @Override
    public boolean isConnected() {
        return false;
    }


    /**
     * Method adds a sai graph to the neo4j database using
     * Cypher queries.
     * @param saiGraph Graph
     * @return int
     */
    @Override
    public int addGraph(Graph saiGraph) {
        try (Session session = driver.session()){
            StatementResult result = session.run("MATCH (g:Graph) return MAX(g.gid)");
            int currentHighestGId = 0;
            Record record = result.single();
            if(record.get("MAX(g.gid)").asObject() != null){
                currentHighestGId = record.get("MAX(g.gid)").asInt();
            }
            final int newGraphId = currentHighestGId + 1;


            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE (g:Graph { gid: {gid} })", parameters("gid", newGraphId));
                tx.success();  // Mark this write as successful.

                saiGraph.getFeatures().forEach(f -> {
                    tx.run("MATCH (g:Graph { gid: {gid} }) SET g." + f.getName() + "= {fValue}",
                            parameters("gid", newGraphId, "fValue", f.getValue()));
                });
                saiGraph.getNodeIDs().forEach(nid -> {
                    tx.run("CREATE (n:Node { nid: {nid} })", parameters("nid", nid));
                    saiGraph.getNodeFeatures(nid).forEach(nf -> {
                        tx.run("MATCH (n:Node { nid: {nid} }) SET n." + nf.getName() + "= {fValue}",
                                parameters("nid", nid, "fValue", nf.getValue()));
                        tx.run("MATCH (n:Node { nid: {nid} }), (g:Graph { gid: {gid} }) CREATE (g)-[:HAS_NODE]->(n)",
                                parameters("nid", nid, "gid", newGraphId));
                    });
                });
                saiGraph.getEdgeIDs().forEach(eid -> {
                    int nid1 = saiGraph.getEdgeSourceNodeID(eid);
                    int nid2 = saiGraph.getEdgeTargetNodeID(eid);
                    tx.run("MATCH (n1:Node { nid: {nid1} }), (n2:Node {nid: {nid2} })" +
                                    "CREATE (n1)-[:HAS_EDGE { eid: {eid} }]->(n2)",
                            parameters("nid1", nid1, "nid2", nid2, "eid", eid));
                    saiGraph.getEdgeFeatures(eid).forEach(ef -> {
                        tx.run("MATCH (n1:Node { nid: {nid1} })-[r:HAS_EDGE]->(n2:Node { nid: {nid2} }) SET r." + ef.getName() + " = {fValue}",
                                parameters("nid1", nid1, "nid2", nid2, "eid", eid,"fValue", ef.getValue()));
                    });
                });
                tx.success();
            } catch (Exception e) {
                e.printStackTrace();
                //logger.error("\nError Cause: " + e.getCause() + "\nError Message: " + e.getMessage());
            }
            return newGraphId;
        }
    }

    /**
     * Method calls the Graph Factory to retrieve a graph from the neo4j db by id.
     * @param graphId int
     * @param graphFactory GraphFactory
     * @param <G> Graph
     * @return
     */
    @Override
    public <G extends net.sourcedestination.sai.graph.Graph> G retrieveGraph(int graphId, GraphFactory<G> graphFactory) {
        net.sourcedestination.sai.neo4jplugin.db.GraphFactory gFactory = new net.sourcedestination.sai.neo4jplugin.db.GraphFactory(driver);

        return (G) gFactory.retrieveGraph(graphId);
    }

    @Override
    public void deleteGraph(int i) {

    }

    @Override
    public int getDatabaseSize() {
        return 0;
    }

    @Override
    public Stream<Integer> getGraphIDStream() {
        return null;
    }

    @Override
    public Stream<Integer> retrieveGraphsWithFeature(net.sourcedestination.sai.graph.Feature feature) {
        return null;
    }

    @Override
    public Stream<Integer> retrieveGraphsWithFeatureName(String s) {
        return null;
    }

    public static void main(String[] args) {
        Neo4jDBInterface ifs = new Neo4jDBInterface();
        MutableGraph graph = new MutableGraph();
        Feature feature1 = new Feature("label", "Value_1");
        Feature feature2 = new Feature("label", "Value_2");
        Feature feature3 = new Feature("label", "Value_3");
        Feature feature4 = new Feature("label", "Value_4");
        Feature feature5 = new Feature("label", "Value_5");
        Feature feature6 = new Feature("label", "Value_6");
        graph.addFeature(feature1);
        graph.addFeature(feature2);
        graph.addNode(1);
        graph.addNode(2);
        graph.addNodeFeature(1, feature3);
        graph.addNodeFeature(2, feature4);
        graph.addEdge(1, 1, 2);
        graph.addEdgeFeature(1, feature5);
        graph.addEdgeFeature(1, feature6);
        ifs.addGraph(graph);
        /*
        ifs.retrieveGraph(1, new GraphFactory<Graph>() {
            @Override
            public Graph copy(Graph graph) {
                return null;
            }
        });     */
    }
}
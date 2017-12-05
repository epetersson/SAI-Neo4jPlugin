package net.sourcedestination.sai.neo4jplugin.db;

import net.sourcedestination.sai.db.DBInterface;
import net.sourcedestination.sai.graph.Feature;
import net.sourcedestination.sai.graph.Graph;
import net.sourcedestination.sai.graph.GraphFactory;
import net.sourcedestination.sai.graph.MutableGraph;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.GraphDatabase;

import java.util.stream.Stream;
import java.io.File;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by ep on 2017-11-27.
 */
public class Neo4jDBInterface implements DBInterface {
    Driver driver;
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

            result = session.run("MATCH (f:Feature) return MAX(f.fid);");
            record = result.single();
            int currentHighestFId = 0;
            if(record.get("MAX(f.fid)").asObject() != null){
                currentHighestFId = record.get("MAX(f.fid)").asInt();
            }
            final int[] newFeatureId = {currentHighestFId};

            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE (g:Graph { gid: {gid} })", parameters("gid", newGraphId));
                tx.success();  // Mark this write as successful.

                saiGraph.getFeatures().forEach(f -> {
                    newFeatureId[0]++;
                    tx.run("CREATE (f:Feature {fid: {fid}, name: {n}, value: {v}})",
                            parameters("fid", newFeatureId[0], "n", f.getName(), "v", f.getValue()));
                    tx.run("MATCH (g:Graph { gid: {gid} }), (f:Feature { fid: {fid} }) CREATE (g)-[:HAS_FEATURE]->(f)",
                            parameters("gid", newGraphId, "fid", newFeatureId[0]));
                });
                saiGraph.getNodeIDs().forEach(nid -> {
                    tx.run("CREATE (n:Node { nid: {nid} })", parameters("nid", nid));
                    saiGraph.getNodeFeatures(nid).forEach(nf -> {
                        newFeatureId[0]++;
                        tx.run("CREATE (f:Feature {fid: {fid}, name: {n}, value: {v}})",
                                parameters("fid", newFeatureId[0], "n", nf.getName(), "v", nf.getValue()));
                        tx.run("MATCH (n:Node { nid: {nid} }), (f:Feature { fid: {fid} }) CREATE (n)-[:HAS_FEATURE]->(f)",
                                parameters("nid", nid, "fid", newFeatureId[0]));
                        tx.run("MATCH (n:Node { nid: {nid} }), (g:Graph { gid: {gid} }) CREATE (g)-[:HAS_NODE]->(n)",
                                parameters("nid", nid, "gid", newGraphId));
                    });
                });
                saiGraph.getEdgeIDs().forEach(eid -> {
                    int nid1 = saiGraph.getEdgeSourceNodeID(eid);
                    int nid2 = saiGraph.getEdgeTargetNodeID(eid);
                    tx.run("CREATE (e:Edge { eid: {eid}, nid1: {nid1}, nid2: {nid2} })",
                            parameters("eid", eid, "nid1", nid1, "nid2", nid2));
                    tx.run("MATCH (e:Edge { eid: {eid} }), (n1:Node {nid: e.nid1}) CREATE (n1)-[:HAS_EDGE]->(e)",
                            parameters("eid", eid));
                    tx.run("MATCH (e:Edge { eid: {eid} }), (n2:Node {nid: e.nid2}) CREATE (n2)-[:HAS_EDGE]->(e)",
                            parameters("eid", eid));
                    saiGraph.getEdgeFeatures(eid).forEach(ef -> {
                        newFeatureId[0]++;
                        tx.run("CREATE (f:Feature {fid: {fid}, name: {n}, value: {v}})",
                                parameters("fid", newFeatureId[0], "n", ef.getName(), "v", ef.getValue()));
                        tx.run("MATCH (e:Edge { eid: {eid} }), (f:Feature { fid: {fid} }) CREATE (e)-[:HAS_FEATURE]->(f)",
                                parameters("eid", eid, "fid", newFeatureId[0]));

                    });
                });
                tx.success();
            } catch (Exception e) {
                e.printStackTrace();
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

    public static void main(String[] args){
        Neo4jDBInterface ifs = new Neo4jDBInterface();
        MutableGraph graph = new MutableGraph();
        Feature feature1 = new Feature("Name 1", "Value 1");
        Feature feature2 = new Feature("Name 2", "Value 2");
        Feature feature3 = new Feature("Name 3", "Value 3");
        Feature feature4 = new Feature("Name 4", "Value 4");
        Feature feature5 = new Feature("Name 5", "Value 5");
        Feature feature6 = new Feature("Name 6", "Value 6");
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

        ifs.retrieveGraph(1, new GraphFactory<Graph>() {
            @Override
            public Graph copy(Graph graph) {
                return null;
            }
        });
    }
}

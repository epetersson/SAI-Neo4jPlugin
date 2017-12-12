package net.sourcedestination.sai.neo4jplugin.db;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import net.sourcedestination.sai.db.DBInterface;
import net.sourcedestination.sai.graph.Feature;
import net.sourcedestination.sai.graph.Graph;
import net.sourcedestination.sai.graph.GraphFactory;
import net.sourcedestination.sai.graph.MutableGraph;
import net.sourcedestination.sai.retrieval.GraphRetriever;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.GraphDatabase;

import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
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
        driver.close();
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
            Set<String> graphFeatureSet = Sets.newHashSet();
            Set<String> nodeFeatureSet = Sets.newHashSet();
            Set<String> edgeFeatureSet = Sets.newHashSet();
            int currentHighestGId = 0;
            Record record = result.single();
            if(record.get("MAX(g.gid)").asObject() != null){
                currentHighestGId = record.get("MAX(g.gid)").asInt();
            }
            final int newGraphId = currentHighestGId + 1;
            final int[] repeatingGraphLabelCounter = {1};
            final int[] repeatingNodeLabelCounter = {1};
            final int[] repeatingEdgeLabelCounter = {1};


            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE (g:Graph { gid: {gid} })", parameters("gid", newGraphId));
                tx.success();  // Mark this write as successful.

                saiGraph.getFeatures().forEach(f -> {
                    if(graphFeatureSet.contains(f.getName())){
                        String newLabelName = f.getName() + "_" + repeatingGraphLabelCounter[0];
                        tx.run("MATCH (g:Graph { gid: {gid} }) SET g." + newLabelName + " = {fValue}",
                                parameters("gid", newGraphId, "fValue", f.getValue()));
                        repeatingGraphLabelCounter[0]++;
                        graphFeatureSet.add(newLabelName);
                    } else {
                        tx.run("MATCH (g:Graph { gid: {gid} }) SET g."+ f.getName() + " = {fValue}",
                                parameters("gid", newGraphId, "fValue", f.getValue()));
                        graphFeatureSet.add(f.getName());
                    }
                });
                saiGraph.getNodeIDs().forEach(nid -> {
                    tx.run("CREATE (n:Node { nid: {nid} })", parameters("nid", nid));
                    saiGraph.getNodeFeatures(nid).forEach(nf -> {
                        if(nodeFeatureSet.contains(nf.getName())){
                            String newLabelName = nf.getName() + "_" + repeatingNodeLabelCounter[0];
                            tx.run("MATCH (n:Node { nid: {nid} }) SET n." + newLabelName + " = {fValue}",
                                    parameters("nid", nid, "fValue", nf.getValue()));
                            nodeFeatureSet.add(newLabelName);
                        } else {
                            tx.run("MATCH (n:Node { nid: {nid} }) SET n." + nf.getName() + " = {fValue}",
                                    parameters("nid", nid, "fValue", nf.getValue()));
                            nodeFeatureSet.add(nf.getName());
                        }
                    });
                    nodeFeatureSet.clear();
                    tx.run("MATCH (n:Node { nid: {nid} }), (g:Graph { gid: {gid} }) CREATE (g)-[:HAS_NODE]->(n)",
                            parameters("nid", nid, "gid", newGraphId));
                });
                saiGraph.getEdgeIDs().forEach(eid -> {
                    int nid1 = saiGraph.getEdgeSourceNodeID(eid);
                    int nid2 = saiGraph.getEdgeTargetNodeID(eid);
                    tx.run("MATCH (n1:Node { nid: {nid1} }), (n2:Node {nid: {nid2} })" +
                                    "CREATE (n1)-[:HAS_EDGE { eid: {eid} }]->(n2)",
                            parameters("nid1", nid1, "nid2", nid2, "eid", eid));
                    saiGraph.getEdgeFeatures(eid).forEach(ef -> {
                        if(edgeFeatureSet.contains(ef.getName())){
                            String newLabelName = ef.getName() + "_" + repeatingEdgeLabelCounter[0];
                            tx.run("MATCH (n1:Node { nid: {nid1} })-[r:HAS_EDGE]->(n2:Node { nid: {nid2} }) SET r." + newLabelName + " = {fValue}",
                                    parameters("nid1", nid1, "nid2", nid2, "eid", eid,"fValue", ef.getValue()));
                            edgeFeatureSet.add(newLabelName);
                        } else {
                            tx.run("MATCH (n1:Node { nid: {nid1} })-[r:HAS_EDGE]->(n2:Node { nid: {nid2} }) SET r." + ef.getName() + " = {fValue}",
                                    parameters("nid1", nid1, "nid2", nid2, "eid", eid,"fValue", ef.getValue()));
                            edgeFeatureSet.add(ef.getName());
                        }
                    });
                    edgeFeatureSet.clear();
                });
                tx.success();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("\nError Cause: " + e.getCause() + "\nError Message: " + e.getMessage());
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
        try (Session session = driver.session()){
            try (Transaction tx = session.beginTransaction()) {
                tx.run("MATCH (g:Graph { gid: {gid} }) DETACH DELETE g", parameters("gid", i));
                tx.success();  // Mark this write as successful.
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("\nError Cause: " + e.getCause() + "\nError Message: " + e.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("\nError Cause: " + e.getCause() + "\nError Message: " + e.getMessage());
        }
    }

    @Override
    public int getDatabaseSize() {
        int dbSize = 0;
        try (Session session = driver.session()){
            StatementResult result = session.run("MATCH (g:Graph) return COUNT(g)");
            while(result.hasNext()){
                Record record = result.next();
                dbSize = record.get("COUNT(g)").asInt();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("\nError Cause: " + e.getCause() + "\nError Message: " + e.getMessage());
        }
        return dbSize;
    }

    @Override
    public Stream<Integer> getGraphIDStream() {
        HashSet<Integer> graphIds = new HashSet<>();
        try (Session session = driver.session()){
            StatementResult result = session.run("MATCH (g:Graph) return g.gid");
            while(result.hasNext()){
                Record record = result.next();
                graphIds.add(record.get("g.gid").asInt());
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("\nError Cause: " + e.getCause() + "\nError Message: " + e.getMessage());
        }
        return graphIds.stream();
    }

    @Override
    public Stream<Integer> retrieveGraphsWithFeature(net.sourcedestination.sai.graph.Feature feature) {
        HashSet<Integer> graphIds = new HashSet<>();
        try (Session session = driver.session()){
            StatementResult result = session.run("MATCH (g:Graph {label: {fvalue}}) return g.gid",
                    parameters("fvalue", feature.getValue()));
            while(result.hasNext()){
                Record record = result.next();
                graphIds.add(record.get("g.gid").asInt());
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("\nError Cause: " + e.getCause() + "\nError Message: " + e.getMessage());
        }
        return graphIds.stream();
    }

    @Override
    public Stream<Integer> retrieveGraphsWithFeatureName(String s) {
        HashSet<Integer> graphIds = new HashSet<>();
        try (Session session = driver.session()){
            StatementResult result = session.run("MATCH (g:Graph {label: {fvalue}}) return g.gid",
                    parameters("fvalue", s));
            while(result.hasNext()){
                Record record = result.next();
                graphIds.add(record.get("g.gid").asInt());
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("\nError Cause: " + e.getCause() + "\nError Message: " + e.getMessage());
        }
        return graphIds.stream();
    }

    public Stream<Integer> path1IndexBasedRetrieval(Graph query, String ... featureNames){
        Multiset<Integer> graphIds = ConcurrentHashMultiset.create();
        query.getEdgeIDs().forEach(eid -> {
            Set<Feature> fromNodeFeatures = Sets.newHashSet();
            Set<Feature> toNodeFeatures = Sets.newHashSet();
            Set<Feature> edgeFeatures = Sets.newHashSet();
            int path1IndexCounter = 1;
            Set<String> featureNamesSet = Arrays.stream(featureNames)
                    .collect(Collectors.toSet());
            query.getEdgeFeatures(eid)
                    .filter(f -> featureNamesSet.contains(f.getName()))
                    .forEach(edgeFeatures::add);
            if(edgeFeatures.size() == 0) edgeFeatures.add(null); //make links without edge features

            query.getNodeFeatures(query.getEdgeSourceNodeID(eid))
                    .filter(f -> featureNamesSet.contains(f.getName()))
                    .forEach(fromNodeFeatures::add);

            query.getNodeFeatures(query.getEdgeTargetNodeID(eid))
                    .filter(f -> featureNamesSet.contains(f.getName()))
                    .forEach(toNodeFeatures::add);
            try (Session session = driver.session()) {
                for(Feature n1f : fromNodeFeatures) {
                    for(Feature n2f : toNodeFeatures) {
                        for(Feature ef : edgeFeatures) {
                            if(edgeFeatures.contains(null)){
                                StatementResult result = session.run(
                                        "MATCH (g:Graph), (n:Node { " + n1f.getName() + ": {n1fV} }), " +
                                                "(n2:Node { nid: {nid2}, " + n2f.getName() + ": {n2fV} }), " +
                                                "(g)-[:HAS_NODE]->(n)-[e:HAS_EDGE]->(n2) " +
                                                "return g.gid",
                                        parameters("n1fV", n1f.getValue(), "n2fV", n2f.getValue()));
                                while(result.hasNext()){
                                    Record record = result.next();
                                    graphIds.add(record.get("g.gid").asInt());
                                }
                            } else {
                                StatementResult result = session.run(
                                        "MATCH (g:Graph), (n:Node { " + n1f.getName() + ": {n1fV} }), " +
                                                "(n2:Node { nid: {nid2}, " + n2f.getName() + ": {n2fV} }), " +
                                                "(g)-[:HAS_NODE]->(n)-[e:HAS_EDGE { " + ef.getName() + ": {efV} }]->(n2) " +
                                                "return g.gid",
                                        parameters("n1fV", n1f.getValue(), "n2fV", n2f.getValue(), "efV", ef.getValue()));
                                while(result.hasNext()){
                                    Record record = result.next();
                                    graphIds.add(record.get("g.gid").asInt());
                                }
                            }

                        }
                    }
                }
            } catch(Exception e){
                e.printStackTrace();
                logger.error("\nError Cause: " + e.getCause() + "\nError Message: " + e.getMessage());
            }
        });

        return graphIds.entrySet().stream() // stream this multiset
                //sort by multiplicity (negated for descending order)
                .sorted((l,r) -> -Integer.compare(l.getCount(), r.getCount()))
                // convert from multiset entries to graph id's
                .map(Multiset.Entry::getElement);
    }

    public static GraphRetriever<Neo4jDBInterface> getPath1Retriever(String ... featureNames){
        return (db, q) -> db.path1IndexBasedRetrieval(q, featureNames);
    }

}
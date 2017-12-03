package net.sourcedestination.sai.neo4jplugin.db;

import net.sourcedestination.sai.db.DBInterface;
import net.sourcedestination.sai.graph.GraphFactory;
import net.sourcedestination.sai.graph.MutableGraph;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.GraphDatabase;

import java.util.*;
import java.util.stream.Stream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.AccessDeniedException;

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

    public Neo4jDBInterface(File dbFile) throws AccessDeniedException {
        this();
        this.dbFile = dbFile;

        if(dbFile == null || !dbFile.exists() || !dbFile.canRead())
            throw new AccessDeniedException(dbFile.getAbsolutePath());
        populateDatabase();
    }

    public void populateDatabase(){
        nextFeatureID = 0;
        nextGraphID = 0;
        BufferedReader in = null;

        try (Session session = driver.session()){
            in = new BufferedReader(new FileReader(dbFile));
            int numFeatureNames = Integer.parseInt(in.readLine());

            for (int i = 0; i < numFeatureNames; i++) {
                //TODO: need to add graph ID to features, nodes and edges..
                Scanner lin = new Scanner(in.readLine());
                lin.useDelimiter(",");
                final String name = lin.next();
                while (lin.hasNext()) {
                    final int fid = lin.nextInt(); //TODO: Check if nextLong works the same as nextInt
                    final String value = lin.next();
                    if (nextFeatureID <= fid) nextFeatureID = fid + 1;
                    try (Transaction tx = session.beginTransaction())
                    {
                        tx.run("CREATE (f:Feature {fid: {fid}, name: {n}, value: {v}})",
                                parameters("fid", fid, "n", name, "v", value));
                        tx.success();  // Mark this write as successful.
                    }
                }
                lin.close();
            }

            //Read in Graphs
            int numGraphs = Integer.parseInt(in.readLine());
            for(int i=0; i<numGraphs; i++) {
                //get general graph into
                String line = in.readLine();
                Scanner lin = new Scanner(line);
                lin.useDelimiter(",");
                int gid = lin.nextInt();
                int numNodes = lin.nextInt();
                int numEdges = lin.nextInt();

                try (Transaction tx = session.beginTransaction())
                {
                    tx.run("CREATE (g:Graph { gid: {gid} })", parameters("gid", gid));
                    tx.success();
                }
                while (lin.hasNext()) {
                    final int fid = lin.nextInt();
                    try (Transaction tx = session.beginTransaction())
                    {
                        tx.run("MATCH (g:Graph { gid: {gid} }), (f:Feature { fid: {fid} } CREATE (g)-[:HAS_FEATURE]->(f)",
                                parameters("gid", gid, "fid", fid));
                        tx.success();
                    }
                }
                lin.close();

                //get graph nodes
                for(int j=0; j<numNodes; j++) {
                    line = in.readLine();
                    lin = new Scanner(line);
                    lin.useDelimiter(",");
                    final int nid = lin.nextInt();

                    try (Transaction tx = session.beginTransaction())
                    {
                        tx.run("CREATE (n:Node { nid: {nid} })", parameters("nid", nid));
                        tx.success();
                    }

                    while(lin.hasNext()) {
                        final int fid = lin.nextInt();
                        try (Transaction tx = session.beginTransaction())
                        {
                            tx.run("MATCH (n:Node { nid: {nid} }), (f:Feature { fid: {fid} } CREATE (n)-[:HAS_FEATURE]->(f)",
                                    parameters("nid", nid, "fid", fid));
                            tx.success();
                        }
                    }

                    lin.close();
                }

                //get graph edges
                for(int j=0; j<numEdges; j++) {
                    lin = new Scanner(in.readLine());
                    lin.useDelimiter(",");

                    final int eid = lin.nextInt();
                    final int nid1 = lin.nextInt();
                    final int nid2 = lin.nextInt();

                    try (Transaction tx = session.beginTransaction())
                    {
                        tx.run("CREATE (e:Edge { eid: {eid}, nid1: {nid1}, nid2: {nid2} })",
                                parameters("eid", eid, "nid1", nid1, "nid2", nid2));
                        tx.success();
                    }

                    while(lin.hasNext()) {
                        final int fid = lin.nextInt();
                        try (Transaction tx = session.beginTransaction())
                        {
                            tx.run("MATCH (e:Edge { eid: {eid} }), (f:Feature { fid: {fid} } CREATE (e)-[:HAS_FEATURE]->(f)",
                                    parameters("eid", eid, "fid", fid));
                            tx.success();
                        }
                    }
                    lin.close();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Override
    public void disconnect() {
    }

    @Override
    public boolean isConnected() {
        return false;
    }


    //TODO: GraphFactory
    @Override
    public int addGraph(net.sourcedestination.sai.graph.Graph saiGraph) {
        //TODO: need to add graph ID to features, nodes and edges..
        /*Graph graph = new Graph();
        try (Session session = driver.session()){
            try (Transaction tx = session.beginTransaction())
            {
                tx.run("CREATE (g:Graph { gid: {gid} })", parameters("gid", gid));
                tx.success();  // Mark this write as successful.
            }
            try (Transaction tx = session.beginTransaction()) {
                saiGraph.getFeatures().forEach(f -> {
                    tx.run("CREATE (f:Feature {fid: {fid}, name: {n}, value: {v}})",
                            parameters("fid", fid, "n", name, "v", value));
                    Feature feature = new Feature(f.getName(), f.getValue());
                    graph.hasFeature(feature);
                });
                saiGraph.getNodeIDs().forEach(nid -> {
                    Node node = new Node();
                    node.setId(nid);
                    saiGraph.getNodeFeatures(nid).forEach(nf -> {
                        node.hasFeature(new Feature(nf.getName(), nf.getValue()));
                    });
                    graph.hasNode(node);
                });
                saiGraph.getEdgeIDs().forEach(eid -> {
                    Edge edge = new Edge();
                    edge.setId(eid);
                    saiGraph.getEdgeFeatures(eid).forEach(ef -> {
                        edge.hasFeature(new Feature(ef.getName(), ef.getValue()));
                    });
                    Node node1 = session.load(Node.class, saiGraph.getEdgeSourceNodeID(eid));
                    Node node2 = session.load(Node.class, saiGraph.getEdgeTargetNodeID(eid));
                    node1.connectedTo(edge);
                    node2.connectedTo(edge);
                    session.save(node1);
                    session.save(node2);
                });
            }

            return graph.getId();
        }*/
        return 0;
    }

    //TODO: Make my graph implement sai graph interface
    @Override
    public <G extends net.sourcedestination.sai.graph.Graph> G retrieveGraph(int graphId, GraphFactory<G> graphFactory) {
        MutableGraph graph = new MutableGraph();
        /*Graph retrievedGraph = session.load(Graph.class, graphId);
        Stream<net.sourcedestination.sai.graph.Feature> graphFeatures = retrievedGraph.getFeatures();
        Set<Node> graphNodes = retrievedGraph.getNodes();
        graphFeatures.forEach(f ->
                graph.addFeature(new net.sourcedestination.sai.graph.Feature(f.getName(), f.getValue())));
        graphNodes.forEach(n -> {
            graph.addNode(n.getId());
            n.getFeatures().forEach(f -> graph.addNodeFeature(n.getId(),
                    new net.sourcedestination.sai.graph.Feature(f.getName(), f.getValue())));
            n.getEdges().forEach(e -> {
                graph.addEdge(e.getId(), e.getNode1Id(), e.getNode2Id());
                e.getFeatures().forEach(f -> graph.addEdgeFeature(e.getId(),
                        new net.sourcedestination.sai.graph.Feature(f.getName(), f.getValue())));
            });
        });*/

        return (G) graph;
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
}

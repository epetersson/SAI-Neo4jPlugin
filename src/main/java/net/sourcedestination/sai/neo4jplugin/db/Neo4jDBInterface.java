package net.sourcedestination.sai.neo4jplugin.db;

import net.sourcedestination.sai.neo4jplugin.Neo4jSessionFactory;
import net.sourcedestination.sai.neo4jplugin.domain.Edge;
import net.sourcedestination.sai.neo4jplugin.domain.Feature;
import net.sourcedestination.sai.neo4jplugin.domain.Graph;
import net.sourcedestination.sai.neo4jplugin.domain.Node;
import net.sourcedestination.sai.db.DBInterface;
import net.sourcedestination.sai.graph.GraphFactory;
import net.sourcedestination.sai.graph.MutableGraph;
import org.neo4j.ogm.session.Session;

import java.util.*;
import java.util.stream.Stream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.AccessDeniedException;

/**
 * Created by ep on 2017-11-27.
 */
public class Neo4jDBInterface implements DBInterface {
    private Neo4jSessionFactory factory;
    private Session session;
    private File dbFile;

    private long nextFeatureID = 1;
    private long nextGraphID = 1;

    public Neo4jDBInterface(){
        this.factory = Neo4jSessionFactory.getInstance();
        this.session = factory.getNeo4jSession();
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

        try {
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
                    Feature feature = new Feature(name, value);
                    feature.setId(fid);
                    session.save(feature);
                    //fRepo.save(feature);
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

                Graph graph = new Graph();
                graph.setId(gid);
                while (lin.hasNext()) {
                    Feature feature = session.load(Feature.class, lin.nextInt());
                    graph.hasFeature(feature);
                }
                lin.close();

                //get graph nodes
                for(int j=0; j<numNodes; j++) {
                    line = in.readLine();
                    lin = new Scanner(line);
                    lin.useDelimiter(",");
                    final int nid = lin.nextInt();
                    Node node = new Node();
                    node.setId(nid);
                    graph.hasNode(node);

                    while(lin.hasNext()) {
                        Feature feature = session.load(Feature.class, lin.nextInt());
                        node.hasFeature(feature);
                        /* TODO: Ask Dr. Morwick about below
                        graphsWithFeatureName.put(f.getName(), gid);
                        graphsWithFeature.put(f, gid);*/
                    }
                    session.save(node);

                    lin.close();
                }

                //get graph edges
                for(int j=0; j<numEdges; j++) {
                    lin = new Scanner(in.readLine());
                    lin.useDelimiter(",");

                    final int eid = lin.nextInt();
                    final int nid1 = lin.nextInt();
                    final int nid2 = lin.nextInt();
                    Node node1 = session.load(Node.class, nid1);
                    Node node2 = session.load(Node.class, nid2);
                    Edge edge = new Edge();

                    edge.setId(eid);
                    node1.connectedTo(edge);
                    node2.connectedTo(edge);
                    edge.setNode1Id(nid1);
                    edge.setNode2Id(nid2);

                    while(lin.hasNext()) {
                        Feature feature = session.load(Feature.class, lin.nextInt());
                        edge.hasFeature(feature);
                    }
                    lin.close();
                }
                session.save(graph); //TODO: Double check if this actually saves the edges aswell. It should.
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Override
    public void disconnect() {
        session.purgeDatabase();
    }

    @Override
    public boolean isConnected() {
        return false;
    }


    //TODO: GraphFactory
    @Override
    public int addGraph(net.sourcedestination.sai.graph.Graph saiGraph) {
        //TODO: need to add graph ID to features, nodes and edges..
        Graph graph = new Graph();
        saiGraph.getFeatures().forEach(f -> {
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

        return graph.getId();
    }

    //TODO: Make my graph implement sai graph interface
    @Override
    public <G extends net.sourcedestination.sai.graph.Graph> G retrieveGraph(int graphId, GraphFactory<G> graphFactory) {
        Graph retrievedGraph = session.load(Graph.class, graphId);
        Stream<net.sourcedestination.sai.graph.Feature> graphFeatures = retrievedGraph.getFeatures();
        Set<Node> graphNodes = retrievedGraph.getNodes();
        MutableGraph graph = new MutableGraph();
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
        });

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

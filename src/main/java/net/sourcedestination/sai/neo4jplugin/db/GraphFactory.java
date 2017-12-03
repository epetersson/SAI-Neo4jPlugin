package net.sourcedestination.sai.neo4jplugin.db;

import net.sourcedestination.sai.graph.Feature;
import net.sourcedestination.sai.graph.Graph;
import net.sourcedestination.sai.graph.MutableGraph;
import net.sourcedestination.sai.neo4jplugin.Neo4jSessionFactory;
import net.sourcedestination.sai.neo4jplugin.domain.Node;
import org.neo4j.ogm.session.Session;

import java.util.Set;
import java.util.stream.Stream;

/**
 * Created by ep on 2017-12-01.
 */
public class GraphFactory<G extends Graph> {
    private Neo4jSessionFactory factory;
    private Session session;

    public GraphFactory(Neo4jSessionFactory factory, Session session){
        this.factory = factory;
        this.session = session;
    }

    public <G extends Graph> G retrieveGraph(int graphID) {
        net.sourcedestination.sai.neo4jplugin.domain.Graph retrievedGraph = session.load(net.sourcedestination.sai.neo4jplugin.domain.Graph.class, graphID);
        Stream<Feature> graphFeatures = retrievedGraph.getFeatures();
        retrievedGraph.getFeatures();
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
        return (G)graph;
    }
}

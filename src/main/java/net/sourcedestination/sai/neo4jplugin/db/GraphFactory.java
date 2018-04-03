package net.sourcedestination.sai.neo4jplugin.db;

import net.sourcedestination.sai.graph.Feature;
import net.sourcedestination.sai.graph.Graph;
import net.sourcedestination.sai.graph.MutableGraph;
import org.apache.log4j.Logger;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import java.util.Iterator;
import java.util.List;
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
    private static final Logger logger = Logger.getLogger(GraphFactory.class);

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
                    "MATCH (g:Graph { gid: {gid} })" +
                            "return g",
                    parameters("gid", graphId));
            Record record = graphResult.next();
            Node graphNode = record.get("g").asNode();

            graphNode.asMap().keySet().forEach(gp -> {
                if(!gp.toString().equals("gid")) {
                    graph.addFeature(new Feature(gp.toString(),
                            graphNode.get(gp.toString()).asString()));
                }
            });



            // Auto-commit transactions are a quick and easy way to wrap a read.
            graphResult = session.run(
                    "MATCH (g:Graph { gid: {graphId} }), (g)-[:HAS_NODE]->(n)" +
                            "return g, n",
                    parameters("graphId", graphId));
            // Each Cypher execution returns a stream of records.
            while (graphResult.hasNext())
            {
                record = graphResult.next();
                Node n = record.get("n").asNode();

                if(!graph.getNodeIDs().anyMatch(id -> id.equals(n.get("nid").asInt())))
                    graph.addNode(n.get("nid").asInt());


                n.asMap().keySet().forEach(np -> {
                    if(!np.toString().equals("nid") && !np.toString().equals("gid")){
                        graph.addNodeFeature(n.get("nid").asInt(), new Feature(np.toString(),
                                n.get(np.toString()).toString()));
                    }
                });

            }



            // Auto-commit transactions are a quick and easy way to wrap a read.
            graphResult = session.run(
                    "MATCH (g:Graph { gid: {graphId} }), (g)-[:HAS_NODE]->(n), (g)-[:HAS_NODE]->(n2), " +
                            "(n)-[e]->(n2) " +
                            "return g, n, e, n2",
                    parameters("graphId", graphId));
            // Each Cypher execution returns a stream of records.
            while (graphResult.hasNext())
            {
                record = graphResult.next();
                Node n = record.get("n").asNode();
                Node n2 = record.get("n2").asNode();
                Relationship edgeNode = record.get("e").asRelationship();



                if(!graph.getEdgeIDs().anyMatch(id -> id.equals(edgeNode.get("eid").asInt())))
                    graph.addEdge(edgeNode.get("eid").asInt(), n.get("nid").asInt(), n2.get("nid").asInt());

                edgeNode.asMap().keySet().forEach(ep -> {
                    if(!ep.toString().equals("eid") && !ep.toString().equals("gid") ) {
                        graph.addEdgeFeature(edgeNode.get("eid").asInt(), new Feature(ep.toString(),
                                edgeNode.get(ep.toString()).asString()) );
                    }
                });

            }
        } catch(Exception e){
            e.printStackTrace();
            logger.error("\nError Cause: " + e.getCause() + "\nError Message: " + e.getMessage());
        }
        return (G) graph;
    }
}

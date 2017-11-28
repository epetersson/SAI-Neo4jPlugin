package net.sourcedestination.sai.neo4jplugin.domain;

import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by ep on 2017-11-27.
 */
@NodeEntity
public class Node extends Entity{
    public Node(){

    }
    @Relationship(type = "CONNECTED_TO", direction = Relationship.UNDIRECTED)
    private Set<Edge> connectedNodes = new HashSet<>();

    //Relationships
    @Relationship(type = "HAS_FEATURE")
    private Set<Feature> features = new HashSet<>();

    public void hasFeature(Feature feature){
        features.add(feature);
    }

    public void connectedTo(Edge edge){
        connectedNodes.add(edge);
    }

    public Set<Edge> getEdges(){
        return this.connectedNodes;
    }

    public Set<Feature> getFeatures(){
        return this.features;
    }
}

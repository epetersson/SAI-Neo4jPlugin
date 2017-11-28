package net.sourcedestination.sai.neo4jplugin.domain;

import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by ep on 2017-11-27.
 */
@NodeEntity
public class Graph extends Entity {
    public Graph(){

    }
    //Relationships
    @Relationship(type = "HAS_NODE")
    private Set<Node> nodes = new HashSet<>();

    @Relationship(type = "HAS_FEATURE")
    private Set<Feature> features = new HashSet<>();

    public void hasFeature(Feature feature){
        features.add(feature);
    }

    public void hasNode(Node node){
        nodes.add(node);
    }

    public Set<Node> getNodes(){
        return this.nodes;
    }

    public Set<Feature> getFeatures() {
        return this.features;
    }
}

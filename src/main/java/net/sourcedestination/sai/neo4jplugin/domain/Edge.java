package net.sourcedestination.sai.neo4jplugin.domain;

import org.neo4j.ogm.annotation.Relationship;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by ep on 2017-11-27.
 */

public class Edge extends Entity {
    public Edge(){

    }
    private Integer node1Id;
    private Integer node2Id;

    @Relationship(type = "HAS_FEATURE")
    private Set<Feature> features = new HashSet<>();

    public Set<Feature> getFeatures(){
        return this.features;
    }

    public void hasFeature(Feature feature){
        features.add(feature);
    }

    public void setNode1Id(Integer node1Id){
        this.node1Id = node1Id;
    }

    public void setNode2Id(Integer node2Id){
        this.node2Id = node2Id;
    }

    public Integer getNode1Id(){
        return this.node1Id;
    }

    public Integer getNode2Id(){
        return this.node2Id;
    }
}

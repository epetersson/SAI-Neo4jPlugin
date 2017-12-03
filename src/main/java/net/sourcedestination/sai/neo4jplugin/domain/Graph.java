package net.sourcedestination.sai.neo4jplugin.domain;

import com.google.common.collect.Sets;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Created by ep on 2017-11-27.
 */
@NodeEntity
public class Graph extends Entity implements net.sourcedestination.sai.graph.Graph {
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

    @Override
    public Stream<Integer> getEdgeIDs() {
        return null;
    }

    @Override
    public Stream<Integer> getNodeIDs() {
        return null;
    }

    public Stream<net.sourcedestination.sai.graph.Feature> getFeatures() {
        return (Stream<net.sourcedestination.sai.graph.Feature>) this.features;
    }

    @Override
    public Stream<net.sourcedestination.sai.graph.Feature> getNodeFeatures(int i) {
        return null;
    }

    @Override
    public Stream<net.sourcedestination.sai.graph.Feature> getEdgeFeatures(int i) {
        return null;
    }

    @Override
    public int getEdgeSourceNodeID(int i) {
        return 0;
    }

    @Override
    public int getEdgeTargetNodeID(int i) {
        return 0;
    }
}

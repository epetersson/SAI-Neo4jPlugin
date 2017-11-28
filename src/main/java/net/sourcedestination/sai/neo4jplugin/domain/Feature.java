package net.sourcedestination.sai.neo4jplugin.domain;

import org.neo4j.ogm.annotation.NodeEntity;

/**
 * Created by ep on 2017-11-27.
 */
@NodeEntity
public class Feature extends Entity{
    private String name;
    private String value;

    public Feature(){

    }

    public Feature(String name, String value){
        this.name = name;
        this.value = value;
    }

    public String getName(){
        return this.name;
    }

    public String getValue(){
        return this.value;
    }
}

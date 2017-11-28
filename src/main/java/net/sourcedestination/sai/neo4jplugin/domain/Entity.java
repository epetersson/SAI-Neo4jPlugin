package net.sourcedestination.sai.neo4jplugin.domain;

import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.Id;

/**
 * Created by ep on 2017-11-27.
 */
public abstract class Entity {
    @Id @GeneratedValue
    private Integer id;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id){
        this.id = id;
    }
}

package net.sourcedestination.sai.neo4jplugin;

import org.neo4j.ogm.config.Configuration;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;

/**
 * Created by ep on 2017-11-27.
 */
public class Neo4jSessionFactory {
    private static Configuration configuration = new Configuration.Builder()
            .uri("bolt://localhost:7687")
            .build();
    private final static SessionFactory sessionFactory = new SessionFactory(configuration, "net.sourcedestination.sai.neo4jplugin.domain");
    private static Neo4jSessionFactory factory = new Neo4jSessionFactory();

    public static Neo4jSessionFactory getInstance() {
        return factory;
    }

    // prevent external instantiation
    private Neo4jSessionFactory() {
    }

    public Session getNeo4jSession() {
        return sessionFactory.openSession();
    }
}

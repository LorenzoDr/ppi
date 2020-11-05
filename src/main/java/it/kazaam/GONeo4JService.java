package it.kazaam;

import org.neo4j.driver.*;

import java.util.List;

import static org.neo4j.driver.Values.parameters;

public class GONeo4JService {
    private final Driver neo4j;

    public GONeo4JService(String uri, String user, String pass) {
        neo4j = GraphDatabase.driver(uri, AuthTokens.basic( user, pass));
    }

    List<List<Long>> findAncestors(long node) {
        try (Session session = neo4j.session())
        {
            return session.writeTransaction(tx -> {
                Result result = tx.run( "MATCH path=(p)-[IS_A*0..]->(:GOTerm{GOid:$0})\n" +
                                            "WHERE NOT (()-[:IS_A]->(p))\n" +
                                            "WITH reduce(ids=[], q in nodes(path) | ids+q.GOid) as result\n" +
                                            "return result",
                        parameters( "0", node ));

                return result.list(record -> record.get(0).asList(Value::asLong));
            });
        }
    }

    List<Long> findSuccessors(long node) {
        try (Session session = neo4j.session())
        {
            return session.writeTransaction(tx -> {
                Result result = tx.run( "MATCH path=(p:GOTerm{GOid:$0})-[:IS_A*]->(f:GOTerm)\n" +
                                        "WHERE NOT ((f)-[:IS_A]->())\n" +
                                        "WITH COLLECT(nodes(path)) as nodi\n" +
                                        "WITH REDUCE(output = [], r IN nodi | output + r) AS flat\n" +
                                        "WITH REDUCE(output = [], r in flat | output + r.GOid) as flat\n" +
                                        "UNWIND flat as elements\n" +
                                        "RETURN distinct elements",
                        parameters( "0", node ));

                return result.list(record -> record.get(0).asLong());
            });
        }
    }

    List<List<Long>> findPathsFromNodeToNode(long start, long end) {
        try (Session session = neo4j.session())
        {
            return session.writeTransaction(tx -> {
                Result result = tx.run( "MATCH path=(p{GOid:$0})-[r:IS_A*0..]->(f:GOTerm{GOid: $1})\n" +
                                            "with reduce(ids=[], n in nodes(path)| ids+n.GOid) as res\n" +
                                            "return res",
                        parameters( "0", start, "1", end ));

                return result.list(record -> record.get(0).asList(Value::asLong));
            });
        }
    }

}
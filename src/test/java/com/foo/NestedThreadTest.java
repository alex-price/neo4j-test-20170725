package com.foo;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.neo4j.support.Neo4jTemplate;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;

public class NestedThreadTest
{
    static GraphDatabaseService graphDb;
    static final String dbPath = System.getProperty( "java.io.tmpdir" );
    static Neo4jTemplate neo4jTemplate;
    static ExecutorService threadPool;

    @BeforeClass
    public static void setup()
    {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( dbPath );
        neo4jTemplate = new Neo4jTemplate( graphDb );
        threadPool = Executors.newFixedThreadPool( 5 );
    }

    @AfterClass
    public static void destroy() throws IOException
    {
        if ( graphDb != null )
        {
            graphDb.shutdown();
            FileUtils.deleteDirectory( new File( dbPath ) );
        }
        if ( threadPool != null )
        {
            threadPool.shutdown();
        }
    }

    /**
     * The following test will pass with any of the following changes:
     * 1. Remove call to neo4jTemplate.save().
     * 2. Remove "child" property from class SampleNode.
     * 3. Remove ":Test" label from query in getUIDs().
     */
    @Test
    public void testNestedThread()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            forkedCreateNode( "a" );
            assertThat( getUIDs(), is( Arrays.asList( "a" ) ) );

            SampleNode node = new SampleNode();
            neo4jTemplate.save( node );

            forkedCreateNode( "b" );
            assertThat( getUIDs(), is( Arrays.asList( "a", "b" ) ) );

            forkedCreateNode( "c" );
            assertThat( getUIDs(), is( Arrays.asList( "a", "b", "c" ) ) );

            tx.success();
        }
    }

    /**
     * Execute task in thread-pool, awaiting completion.
     */
    private void forkedCreateNode( String uid )
    {
        try
        {
            Runnable task = new CreateNode( uid );
            Future taskFuture = threadPool.submit( task );
            taskFuture.get();
        }
        catch ( ExecutionException | InterruptedException ex )
        {
            ex.printStackTrace();
        }
    }

    /**
     * Task to create a node.
     */
    private class CreateNode implements Runnable
    {
        String uid;

        CreateNode( String uid )
        {
            this.uid = uid;
        }

        @Override
        public void run()
        {
            try ( Transaction tx = graphDb.beginTx() )
            {
                graphDb.execute( "CREATE (n:Test {uid: {uid}})", map( "uid", uid ) );
                tx.success();
            }
        }
    }

    /**
     * Return a list of all UIDs, in order of creation.
     */
    private List<String> getUIDs()
    {
        String query = "MATCH (n:Test) WHERE exists(n.uid) WITH n ORDER BY id(n) RETURN n.uid AS uid";
        Result result = graphDb.execute( query );
        List<String> uids = new ArrayList<>();
        while ( result.hasNext() )
        {
            Map<String,Object> row = result.next();
            uids.add( (String) row.get( "uid" ) );
        }
        return uids;
    }

    /**
     * Print all node contents.
     */
    private void printNodes()
    {
        String result = graphDb.execute( "MATCH (n) RETURN n" ).resultAsString();
        System.out.println( result );
    }
}

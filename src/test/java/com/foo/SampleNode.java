package com.foo;

import org.springframework.data.neo4j.annotation.GraphId;
import org.springframework.data.neo4j.annotation.NodeEntity;

@NodeEntity
public class SampleNode
{
    @GraphId
    private Long id;
    private SampleNode child;
}

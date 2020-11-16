/**
* Source operator for consuming incoming events
*
* @author  Raja SP
*/

package com.amazonaws.flinkxmldb.sources;

import java.util.Map;
import java.util.Properties;

import com.amazonaws.flinkxmldb.utils.XMLParser;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class EventSource {
    public static DataStream< java.util.HashMap > getEventSource( StreamExecutionEnvironment env, ParameterTool params  ) {
        String eventsSource = params.get("events.source");
        if( eventsSource.equals( "file" ) ) {
            String filePath = params.get( "events.source.file.name" );
            return env.readTextFile( filePath )
               .map( (stringEvent) -> new XMLParser( stringEvent ).parseXML() )
               .uid( "EventsSource" )
               .name( "Events Source" );
        } else if( eventsSource.equals( "socket" ) ) {
            return env.socketTextStream(
                       params.get("events.source.socket.host"),
                       Integer.parseInt(params.get("events.source.socket.port")), "\n", -1)
                .map( (stringEvent) ->  new XMLParser( stringEvent ).parseXML() )
                .uid( "EventsSourceSocket" )
                .name( "Events Source - Socket" );       
        } else if (eventsSource.equals("kafka")) {
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", params.get("kafka.bootstrap.servers"));
            properties.setProperty("group.id", "events_consumer_group");
            return env.addSource(new FlinkKafkaConsumer<>(
                    params.get("events.source.kafka.topic"),  new SimpleStringSchema(), properties))
                    .map( (stringEvent) ->  new XMLParser( stringEvent ).parseXML() )
                    .returns( java.util.HashMap.class )
                    .uid( "Events Stream" )
                    .name( "Events Streams - Kafka ");
        }
        return null;         
    }
}
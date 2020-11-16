/**
* The main DAG creator for entity analytics
*
* @author  Raja SP
*/

package com.amazonaws.flinkxmldb;

import java.util.Map;
import java.util.Properties;

import com.amazonaws.flinkxmldb.sinks.DBSink;
import com.amazonaws.flinkxmldb.sources.EventSource;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XMLDBJob {

	private static final Logger LOG = LoggerFactory.getLogger(XMLDBJob.class);
	private static ParameterTool parameters = null;

	public static void main( String[] args ) throws Exception {
		loadParameters(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EventSource.getEventSource( env, parameters )
				   .addSink( new DBSink( XMLDBJob.parameters ) );
		env.execute("Flink XML DB");
	}

	private static void loadParameters( String[] args) throws Exception {
		ParameterTool comamndLineParam = ParameterTool.fromArgs( args );
		String propsFile = comamndLineParam.get( "propertiesFile" );
		LOG.info( "Properties file name from command line : " + propsFile );
		parameters = ParameterTool.fromPropertiesFile( propsFile );
		StreamExecutionEnvironment.getExecutionEnvironment().getConfig().setGlobalJobParameters( parameters );
	}
}

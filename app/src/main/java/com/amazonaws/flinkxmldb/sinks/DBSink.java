package com.amazonaws.flinkxmldb.sinks;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.util.Map;

import com.amazonaws.flinkxmldb.utils.XMLParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSink extends RichSinkFunction< java.util.HashMap > {

	private static final Logger LOG = LoggerFactory.getLogger(DBSink.class);
    private PreparedStatement statement;
    private JsonArray mappingList;
    private Connection connection;
    private ParameterTool parameters;
    private String rootTag;
  
    public DBSink(ParameterTool params) {
        this.parameters = params;
	}

	@Override
    public void invoke( java.util.HashMap map ) throws Exception {
        LOG.info( "Input Tuple : " + map.toString() );

        if( connection == null ) 
            this.initialise();

        Map xmlRoot = ( Map )map.get( rootTag );
        for( int i=0; i<mappingList.size(); i++ ) {
            JsonObject aMapping = mappingList.get(i).getAsJsonObject();
            String xmlField = aMapping.get( "xmlField" ).getAsString();
            String dataType = aMapping.get( "dataType" ).getAsString();
            int colIndex = i + 1;
            Object dbValue = XMLParser.convertStringToJavaType( dataType, ( String )xmlRoot.get( xmlField ) );
            if( dataType.equalsIgnoreCase( "int" ) )
                statement.setInt( colIndex, ( Integer )dbValue );
            else if( dataType.equalsIgnoreCase( "long" ) ) 	
                statement.setLong( colIndex, ( Long )dbValue );
            else if( dataType.equalsIgnoreCase( "double" ) ) 	
                statement.setDouble( colIndex, ( Double )dbValue );
            else if( dataType.equalsIgnoreCase( "boolean" ) ) 	
                statement.setBoolean( colIndex, ( Boolean )dbValue );
            else if( dataType.equalsIgnoreCase( "string" ) ) 	
                statement.setString( colIndex, ( String )dbValue );
            else if( dataType.equals( "date" ) )
                statement.setDate( colIndex, ( Date )dbValue );	            
        }    
        if (statement.executeUpdate() <= 0) 
            throw new SQLException("0 rows inserted while trying to insert a new row ::: " + statement.toString() );
        LOG.info( "Inserted Record " + statement.toString() );
    }
  
    @Override
    public void open( Configuration config ) throws Exception {
    }

    private void initialise() throws SQLException, ClassNotFoundException {
        LOG.info( "****************************** Initialising DB SINK ******************************" );
        Class.forName( parameters.get( "JDBC_DRIVER_CLASS" ) );
        connection = DriverManager.getConnection( parameters.get( "DB_URL" ), parameters.get( "DB_USER" ), parameters.get( "DB_PASSWD" ) );
        statement = connection.prepareStatement( parameters.get( "SQL" ) );
        LOG.info( "****************************** DB Connection Established ******************************" );
  
        rootTag = parameters.get( "XMLRoot" );
        LOG.info( "Mapper Config : " + parameters.get( "XMLToDBMapping" ) );
        mappingList = JsonParser.parseString( parameters.get( "XMLToDBMapping" ) ).getAsJsonArray();
        LOG.info( "****************************** Mapping Loaded ******************************" );
        LOG.info( mappingList.toString() );
    }
  }
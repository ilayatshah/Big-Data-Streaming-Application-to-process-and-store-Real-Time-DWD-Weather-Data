package dlr.ts.real5G.com.HiwiProject;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.util.factory.Hints;
import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.gson.JsonPrimitive;

import java.io.Serializable;
import java.util.*;


public class FlinkApp {
    public static void main(String[] args) throws Exception {
    	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the Consumer and create a datastream from the existing topic "dwd_precip_10m" in Kafka
        Properties prop1 = new Properties();
        prop1.setProperty("bootstrap.servers", "bootstrap_server_address");
        prop1.setProperty("group.id", "dwd_precip_10m");
        
        Properties prop2 = new Properties();
        prop2.setProperty("bootstrap.servers", "bootstrap_server_address");
        prop2.setProperty("group.id", "dwd_solar_10m");
        
        Properties prop3 = new Properties();
        prop3.setProperty("bootstrap.servers", "bootstrap_server_address");
        prop3.setProperty("group.id", "dwd_airtemp_10m");
        
        Properties prop4 = new Properties();
        prop4.setProperty("bootstrap.servers", "bootstrap_server_address");
        prop4.setProperty("group.id", "dwd_extreme_temp_10m");
        
        Properties prop5 = new Properties();
        prop5.setProperty("bootstrap.servers", "bootstrap_server_address");
        prop5.setProperty("group.id", "dwd_wind2_m10");
        
        Properties prop6 = new Properties();
        prop6.setProperty("bootstrap.servers", "bootstrap_server_address");
        prop6.setProperty("group.id", "dwd_extreme_wind_10m");
        
        FlinkKafkaConsumer<String> flinkConsumer1 = new FlinkKafkaConsumer<>("dwd_precip_10m", new SimpleStringSchema(), prop1);
        FlinkKafkaConsumer<String> flinkConsumer2 = new FlinkKafkaConsumer<>("dwd_solar_10m", new SimpleStringSchema(), prop2);
        FlinkKafkaConsumer<String> flinkConsumer3 = new FlinkKafkaConsumer<>("dwd_airtemp_10m", new SimpleStringSchema(), prop3);
        FlinkKafkaConsumer<String> flinkConsumer4= new FlinkKafkaConsumer<>("dwd_extreme_temp_10m", new SimpleStringSchema(), prop4);
        FlinkKafkaConsumer<String> flinkConsumer5 = new FlinkKafkaConsumer<>("dwd_wind2_m10", new SimpleStringSchema(), prop5);
        FlinkKafkaConsumer<String> flinkConsumer6 = new FlinkKafkaConsumer<>("dwd_extreme_wind_10m", new SimpleStringSchema(), prop6);
            
        flinkConsumer1.setStartFromTimestamp(Long.parseLong("0"));
        flinkConsumer2.setStartFromTimestamp(Long.parseLong("0"));
        flinkConsumer3.setStartFromTimestamp(Long.parseLong("0"));
        flinkConsumer4.setStartFromTimestamp(Long.parseLong("0"));
        flinkConsumer5.setStartFromTimestamp(Long.parseLong("0"));
        flinkConsumer6.setStartFromTimestamp(Long.parseLong("0"));
        DataStream<String> stream1 = env.addSource(flinkConsumer1);
        DataStream<String> stream2 = env.addSource(flinkConsumer2);
        DataStream<String> stream3 = env.addSource(flinkConsumer3);
        DataStream<String> stream4 = env.addSource(flinkConsumer4);
        DataStream<String> stream5= env.addSource(flinkConsumer5);
        DataStream<String> stream6 = env.addSource(flinkConsumer6);
        
        DataStream<String> allStreams = stream1.union(stream2,stream3,stream4,stream5,stream6);
        
        // here the data source will allStreams which are multiple topics 
        allStreams.rebalance().map(new RichMapFunction<String, String>() {

            private static final long serialVersionUID = -2547883355L; // random number

            // Define all the global variables here which will be used in this application
            // variables to store Kafka msg processing summary
            private long numberOfMessagesProcessed;
            private long numberOfMessagesFailed;
            private long numberOfMessagesSkipped;

            // Features list of SimpleFeatures to store Precipitation_topic messages
            List<SimpleFeature> dwd_live_features_1;
            SimpleFeatureType sft_1;
            SimpleFeatureBuilder SFbuilder_1;
            SimpleFeatureStore producerFS_1;
            
            //  Features list of SimpleFeatures to store Solar_topic messages
            List<SimpleFeature> dwd_live_features_2;
            SimpleFeatureType sft_2;
            SimpleFeatureBuilder SFbuilder_2;
            SimpleFeatureStore producerFS_2;
            
            List<SimpleFeature> dwd_live_features_3;
            SimpleFeatureType sft_3;
            SimpleFeatureBuilder SFbuilder_3;
            SimpleFeatureStore producerFS_3;
            
            List<SimpleFeature> dwd_live_features_4;
            SimpleFeatureType sft_4;
            SimpleFeatureBuilder SFbuilder_4;
            SimpleFeatureStore producerFS_4;
            
            List<SimpleFeature> dwd_live_features_5;
            SimpleFeatureType sft_5;
            SimpleFeatureBuilder SFbuilder_5;
            SimpleFeatureStore producerFS_5;
            
            List<SimpleFeature> dwd_live_features_6;
            SimpleFeatureType sft_6;
            SimpleFeatureBuilder SFbuilder_6;
            SimpleFeatureStore producerFS_6;

            @Override
            public void open(Configuration parameters) {
                System.out.println("In open method.");

                //define connection parameters to Kafka Producer topic
                Map<String, Serializable> params = new HashMap<>();
                params.put("kafka.brokers","kafka_broker_address");
                params.put("kafka.zookeepers","zookeeper_address");
                params.put("kafka.consumer.count", "0");

                //connect to kafka datastore producer_1
                try {
                    DataStore producer = DataStoreFinder.getDataStore(params);
                    
                    if (producer==null) {
                        System.out.println("Error fetching datastore1");
                    } else {
                        System.out.println("Successfully connected to the Producer1");
                    }
                    
                    // connect to kafka datastore producer_2
                    DataStore producer_2 = DataStoreFinder.getDataStore(params);
                    
                    if (producer_2==null) {
                        System.out.println("Error fetching datastore2");
                    } else {
                        System.out.println("Successfully connected to the Producer2");
                    }
                    
                    // connect to kafka datastore producer_3
                    DataStore producer_3 = DataStoreFinder.getDataStore(params);
                    
                    if (producer_3==null) {
                        System.out.println("Error fetching datastore3");
                    } else {
                        System.out.println("Successfully connected to the Producer3");
                    }
                    
                    // connect to kafka datastore producer_4
                    DataStore producer_4 = DataStoreFinder.getDataStore(params);
                    
                    if (producer_4==null) {
                        System.out.println("Error fetching datastore4");
                    } else {
                        System.out.println("Successfully connected to the Producer4");
                    }
                    
                    // connect to kafka datastore producer_5
                    DataStore producer_5 = DataStoreFinder.getDataStore(params);
                    
                    if (producer_5==null) {
                        System.out.println("Error fetching datastore5");
                    } else {
                        System.out.println("Successfully connected to the Producer5");
                    }
                    
                    // connect to kafka datastore producer_6
                    DataStore producer_6 = DataStoreFinder.getDataStore(params);
                    
                    if (producer_6==null) {
                        System.out.println("Error fetching datastore6");
                    } else {
                        System.out.println("Successfully connected to the Producer6");
                    }

                    //create simple feature types, if it already exists, make sure it matches with the existing sft.
                    // If not, new and old features will not be stored together
                    StringBuilder attributes = new StringBuilder();
                    attributes.append("FID:Integer,");
                    attributes.append("STATIONS_ID:Long,");
                    attributes.append("MESS_DATUM:Long,");
                    attributes.append("RWS_DAU_10:Long,");
                    attributes.append("RWS_IND_10:Long,");
                    attributes.append("RWS_10:Long,");
                    attributes.append("*StationLocation:Point:srid=4326");
                    
                    // here dwd_precip_10m_geomesa is the SFT which we have created in the geomesa
                    sft_1 = SimpleFeatureTypes.createType("dwd_precip_10m_geomesa",attributes.toString());
                    
                    
                    SFbuilder_1 = new SimpleFeatureBuilder(sft_1); //feature builder
					assert producer != null;
                    producer.createSchema(sft_1); //created if it doesn't exist. If not, execution of this statement does nothing

                    
                    //create and add features to kafka
                    producerFS_1 = (SimpleFeatureStore) producer.getFeatureSource(sft_1.getTypeName());
                    dwd_live_features_1 = new ArrayList<>();
                    

                    //create simple feature types, if it already exists, make sure it matches with the existing sft.
                    // If not, new and old features will not be stored together
                    StringBuilder attributes_2 = new StringBuilder();
                    attributes_2.append("FID:Integer,");
                    attributes_2.append("STATIONS_ID:Long,");
                    attributes_2.append("MESS_DATUM:Long,");
                    attributes_2.append("GS_10:Long,");
                    attributes_2.append("LS_10:Long,");
                    attributes_2.append("DS_10:Long,");
                    attributes_2.append("SD_10:Long,");
                    attributes_2.append("*StationLocation:Point:srid=4326");
                    //geomesa-da-kafka-<sftName> would be the name of the kafka topic

                    sft_2 = SimpleFeatureTypes.createType("dwd_solar_10m_geomesa",attributes_2.toString());
                    SFbuilder_2 = new SimpleFeatureBuilder(sft_2); //feature builder
					assert producer_2 != null;
                    producer_2.createSchema(sft_2); //created if it doesn't exist. If not, execution of this statement does nothing
                    
                    //create and add features to kafka
                    producerFS_2 = (SimpleFeatureStore) producer_2.getFeatureSource(sft_2.getTypeName());
                    dwd_live_features_2 = new ArrayList<>();
                    
                    //create simple feature types, if it already exists, make sure it matches with the existing sft.
                    // If not, new and old features will not be stored together
                    StringBuilder attributes_3 = new StringBuilder();
                    attributes_3.append("FID:Integer,");
                    attributes_3.append("STATIONS_ID:Long,");
                    attributes_3.append("MESS_DATUM:Long,");
                    attributes_3.append("PP_10:Long,");
                    attributes_3.append("TD_10:Long,");
                    attributes_3.append("RF_10:Long,");
                    attributes_3.append("TM5_10:Long,");
                    attributes_3.append("TT_10:Long,");
                    attributes_3.append("*StationLocation:Point:srid=4326");
                    //geomesa-da-kafka-<sftName> would be the name of the kafka topic

                    sft_3 = SimpleFeatureTypes.createType("dwd_airtemp_10m_geomesa",attributes_3.toString());
                    SFbuilder_3 = new SimpleFeatureBuilder(sft_3); //feature builder
					assert producer_3 != null;
                    producer_3.createSchema(sft_3); //created if it doesn't exist. If not, execution of this statement does nothing

                  //create and add features to kafka
                    producerFS_3 = (SimpleFeatureStore) producer_3.getFeatureSource(sft_3.getTypeName());
                    dwd_live_features_3 = new ArrayList<>();
                    
                    //create simple feature types, if it already exists, make sure it matches with the existing sft.
                    // If not, new and old features will not be stored together
                    StringBuilder attributes_4 = new StringBuilder();
                    attributes_4.append("FID:Integer,");
                    attributes_4.append("STATIONS_ID:Long,");
                    attributes_4.append("MESS_DATUM:Long,");
                    attributes_4.append("TX_10:Long,");
                    attributes_4.append("TX5_10:Long,");
                    attributes_4.append("TN_10:Long,");
                    attributes_4.append("TN5_10:Long,");
                    attributes_4.append("*StationLocation:Point:srid=4326");
                    //geomesa-da-kafka-<sftName> would be the name of the kafka topic

                    sft_4 = SimpleFeatureTypes.createType("dwd_extreme_temp_10m_geomesa",attributes_4.toString());
                    SFbuilder_4 = new SimpleFeatureBuilder(sft_4); //feature builder
					assert producer_4 != null;
                    producer_4.createSchema(sft_4); //created if it doesn't exist. If not, execution of this statement does nothing
                  
                    //create and add features to kafka
                    producerFS_4 = (SimpleFeatureStore) producer_4.getFeatureSource(sft_4.getTypeName());
                    dwd_live_features_4 = new ArrayList<>();
                    
                    //create simple feature types, if it already exists, make sure it matches with the existing sft.
                    // If not, new and old features will not be stored together
                    StringBuilder attributes_5 = new StringBuilder();
                    attributes_5.append("FID:Integer,");
                    attributes_5.append("STATIONS_ID:Long,");
                    attributes_5.append("MESS_DATUM:Long,");
                    attributes_5.append("FF_10:Long,");
                    attributes_5.append("DD_10:Long,");
                    
                    attributes_5.append("*StationLocation:Point:srid=4326");
                    //geomesa-da-kafka-<sftName> would be the name of the kafka topic

                    sft_5 = SimpleFeatureTypes.createType("dwd_wind2_m10_geomesa",attributes_5.toString());
                    SFbuilder_5 = new SimpleFeatureBuilder(sft_5); //feature builder
					assert producer_5 != null;
                    producer_5.createSchema(sft_5); //created if it doesn't exist. If not, execution of this statement does nothing
                    
                  //create and add features to kafka
                    producerFS_5 = (SimpleFeatureStore) producer_5.getFeatureSource(sft_5.getTypeName());
                    dwd_live_features_5 = new ArrayList<>();
                    
                    //create simple feature types, if it already exists, make sure it matches with the existing sft.
                    // If not, new and old features will not be stored together
                    StringBuilder attributes_6 = new StringBuilder();
                    attributes_6.append("FID:Integer,");
                    attributes_6.append("STATIONS_ID:Long,");
                    attributes_6.append("MESS_DATUM:Long,");
                    attributes_6.append("FX_10:Long,");
                    attributes_6.append("FNX_10:Long,");
                    attributes_6.append("FMX_10:Long,");
                    attributes_6.append("DX_10:Long,");
                    attributes_6.append("*StationLocation:Point:srid=4326");

                    sft_6 = SimpleFeatureTypes.createType("dwd_extreme_wind_10m_geomesa",attributes_6.toString());
                    SFbuilder_6 = new SimpleFeatureBuilder(sft_6); //feature builder
					assert producer_6 != null;
                    producer_6.createSchema(sft_6); //created if it doesn't exist. If not, execution of this statement does nothing

                  //create and add features to kafka
                    producerFS_6 = (SimpleFeatureStore) producer_6.getFeatureSource(sft_6.getTypeName());
                    dwd_live_features_6 = new ArrayList<>();

                  } catch (Exception e) {
                    System.out.println("Something went wrong in Open Method....");  
                    }
                
                //initialise the defined variables
                numberOfMessagesProcessed = 0;
                numberOfMessagesFailed = 0;
                numberOfMessagesSkipped = 0;
            }

            public String map(String valueFromKafka) {
                // this function runs for every message from kafka topic

                // check if the message has any content
                if (valueFromKafka == null || valueFromKafka.trim().isEmpty()) {
                    System.out.println("Message from Kafka is empty!");
                    numberOfMessagesSkipped++;
                    return "Warning: Message from Kafka is empty";
                } else {
                	try {
                	// create JSON object (kafka message) and get its elements
                	JSONObject json_obj = new JSONObject(valueFromKafka);
                    JSONArray features = json_obj.getJSONArray("features");
                    
                    // loop this process and put these values in a separate JSON Object 
                        for (int i = 0; i < features.length(); i++) {
                        JSONObject feature = features.getJSONObject(i);
                         Long STATIONS_ID = feature.getJSONObject("properties").getLong("STATIONS_ID");

                          //int STATIONS_ID = feature.getJSONObject("properties").getInt("STATIONS_ID");
                          Long MESS_DATUM = feature.getJSONObject("properties").getLong("MESS_DATUM");
                          JSONArray coordJson = feature.getJSONObject("geometry").getJSONArray("coordinates");
                          String pointWKT = "POINT(" + coordJson.getString(0) + " " + coordJson.getString(1) + ")";

                        // extract a unique fid 
                        String fid = STATIONS_ID+"_"+MESS_DATUM;
                        
                        if(feature.getJSONObject("properties").has("RWS_DAU_10")) {
                      	  //preciptation message
                      	    Long RWS_DAU_10 = feature.getJSONObject("properties").optLong("RWS_DAU_10");
                            Long RWS_IND_10 = feature.getJSONObject("properties").optLong("RWS_IND_10");
                            Long RWS_10 = feature.getJSONObject("properties").optLong("RWS_10");
                            
                            // create and add feature to the dwd_live_features_1 (SFbuilder_1)
                            SFbuilder_1.set("FID", fid);
                            SFbuilder_1.set("STATIONS_ID", STATIONS_ID);
                            SFbuilder_1.set("MESS_DATUM", MESS_DATUM);
                            SFbuilder_1.set("RWS_DAU_10", RWS_DAU_10);
                            SFbuilder_1.set("RWS_10", RWS_10);
                            SFbuilder_1.set("RWS_IND_10", RWS_IND_10);
                            SFbuilder_1.set("StationLocation", pointWKT);
                            SFbuilder_1.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                            dwd_live_features_1.add(SFbuilder_1.buildFeature(fid));                              
                         
                        }
                          
                        else if(feature.getJSONObject("properties").has("DS_10")) {
                        	  //solar message
                        	  Long DS_10 = feature.getJSONObject("properties").getLong("DS_10");
                              Long GS_10 = feature.getJSONObject("properties").getLong("GS_10");
                              Long SD_10 = feature.getJSONObject("properties").getLong("SD_10");
                              Long LS_10 = feature.getJSONObject("properties").getLong("LS_10");
                        	  
                              // create and add feature to the dwd_live_features_2 (SFbuilder_2)
                              SFbuilder_2.set("FID", fid);
                              SFbuilder_2.set("STATIONS_ID", STATIONS_ID);
                              SFbuilder_2.set("MESS_DATUM", MESS_DATUM);
                              SFbuilder_2.set("DS_10", DS_10);
                              SFbuilder_2.set("GS_10", GS_10);
                              SFbuilder_2.set("LS_10", LS_10);
                              SFbuilder_2.set("SD_10", SD_10);
                              SFbuilder_2.set("StationLocation", pointWKT);
                              SFbuilder_2.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                              dwd_live_features_2.add(SFbuilder_2.buildFeature(fid));                              
                          }
                            
                          else if(feature.getJSONObject("properties").has("PP_10")) {
                        	  //air_temperature message
                        	  Long PP_10 = feature.getJSONObject("properties").optLong("PP_10");
                              Long TD_10 = feature.getJSONObject("properties").optLong("TD_10");
                              Long RF_10 = feature.getJSONObject("properties").optLong("RF_10");
                              Long TM5_10 = feature.getJSONObject("properties").optLong("TM5_10");
                              Long TT_10 = feature.getJSONObject("properties").optLong("TT_10");
                              
                              // create and add feature to the dwd_live_features_3 (SFbuilder_3)
                              SFbuilder_3.set("FID", fid);
                              SFbuilder_3.set("STATIONS_ID", STATIONS_ID);
                              SFbuilder_3.set("MESS_DATUM", MESS_DATUM);
                              SFbuilder_3.set("PP_10", PP_10);
                              SFbuilder_3.set("TD_10", TD_10);
                              SFbuilder_3.set("RF_10", RF_10);
                              SFbuilder_3.set("TM5_10", TM5_10);
                              SFbuilder_3.set("TT_10", TT_10);
                              SFbuilder_3.set("StationLocation", pointWKT);
                              SFbuilder_3.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                              dwd_live_features_3.add(SFbuilder_3.buildFeature(fid));                         
                        }
                            
                          else if(feature.getJSONObject("properties").has("TX_10")) {
                        	  //extreme_temperature message
                        	  Long TX_10 = feature.getJSONObject("properties").optLong("TX_10");
                              Long TX5_10 = feature.getJSONObject("properties").optLong("TX5_10");
                              Long TN_10 = feature.getJSONObject("properties").optLong("TN_10");
                              Long TN5_10 = feature.getJSONObject("properties").optLong("TN5_10");
                              
                              
                              // create and add feature to the dwd_live_features_4 (SFbuilder_4)
                              SFbuilder_4.set("FID", fid);
                              SFbuilder_4.set("STATIONS_ID", STATIONS_ID);
                              SFbuilder_4.set("MESS_DATUM", MESS_DATUM);
                              SFbuilder_4.set("TX_10", TX_10);
                              SFbuilder_4.set("TX5_10", TX5_10);
                              SFbuilder_4.set("TN_10", TN_10);
                              SFbuilder_4.set("TN5_10", TN5_10);   
                              SFbuilder_4.set("StationLocation", pointWKT);
                              SFbuilder_4.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                              dwd_live_features_4.add(SFbuilder_4.buildFeature(fid));
                          }
                              
                              else if(feature.getJSONObject("properties").has("FF_10")) {
                            	  //wind message
                            	  Long FF_10 = feature.getJSONObject("properties").optLong("FF_10");
                                  Long DD_10 = feature.getJSONObject("properties").optLong("DD_10");
                                  // create and add feature to the dwd_live_features_5 (SFbuilder_5)
                                  SFbuilder_5.set("FID", fid);
                                  SFbuilder_5.set("STATIONS_ID", STATIONS_ID);
                                  SFbuilder_5.set("MESS_DATUM", MESS_DATUM);
                                  SFbuilder_5.set("FF_10", FF_10);
                                  SFbuilder_5.set("DD_10", DD_10);
                                                                                                  
                                  SFbuilder_5.set("StationLocation", pointWKT);
                                  SFbuilder_5.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                                  dwd_live_features_5.add(SFbuilder_5.buildFeature(fid));
                              }

                              else if(feature.getJSONObject("properties").has("FX_10")) {
                            	  //extreme_wind message
                            	  Long FX_10 = feature.getJSONObject("properties").optLong("FX_10");
                                  Long FNX_10 = feature.getJSONObject("properties").optLong("FNX_10");
                                  Long FMX_10 = feature.getJSONObject("properties").optLong("FMX_10");
                                  Long DX_10 = feature.getJSONObject("properties").optLong("DX_10");
                                                                   
                                  
                                  // create and add feature to the dwd_live_features_6 (SFbuilder_6)
                                  SFbuilder_6.set("FID", fid);
                                  SFbuilder_6.set("STATIONS_ID", STATIONS_ID);
                                  SFbuilder_6.set("MESS_DATUM", MESS_DATUM);
                                  //SFbuilder.set("QN", QN);
                                  SFbuilder_6.set("FX_10", FX_10);
                                  SFbuilder_6.set("FNX_10", FNX_10);
                                  SFbuilder_6.set("FMX_10", FMX_10);
                                  SFbuilder_6.set("DX_10", DX_10);
                                                                                                  
                                  SFbuilder_6.set("StationLocation", pointWKT);
                                  SFbuilder_6.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                                  dwd_live_features_6.add(SFbuilder_6.buildFeature(fid));
                          
                              }
                        }

                         // write these simple features to the kafka topic Precipitation
                            
                                System.out.println("Writing " + dwd_live_features_1.size()
                                        + " features to kafka topic PRECIPITAION");

                                for (SimpleFeature f : dwd_live_features_1) {
                                    producerFS_1.addFeatures(new ListFeatureCollection(sft_1, Collections.singletonList(f)));
                                }
                                dwd_live_features_1.clear();

                         // empty the array for the next msg: will be writing new feature to kafka wd no duplicate
                         // write these simple features to the kafka topic Solar
                            
                                System.out.println("Writing " + dwd_live_features_2.size()
                                        + " features to kafka topic SOLAR");
                                for (SimpleFeature f2 : dwd_live_features_2) {
                                    producerFS_2.addFeatures(new ListFeatureCollection(sft_2, Collections.singletonList(f2)));
                                }
                                dwd_live_features_2.clear();

                               // same is here
                              // write these simple features to the kafka topic AIR_TEMPERATURE
                                 
                                     System.out.println("Writing " + dwd_live_features_3.size()
                                             + " features to kafka topic AIR_TEMPERATURE");
                                     for (SimpleFeature f3 : dwd_live_features_3) {
                                         producerFS_3.addFeatures(new ListFeatureCollection(sft_3, Collections.singletonList(f3)));
                                     }
                                     dwd_live_features_3.clear();  
                                 
                              // same is here
                              // write these simple features to the kafka topic EXTREME_TEMPERATURE
                                 
                                     System.out.println("Writing " + dwd_live_features_4.size()
                                             + " features to kafka topic EXTREME_TEMPERATURE");
                                     for (SimpleFeature f4 : dwd_live_features_4) {
                                         producerFS_4.addFeatures(new ListFeatureCollection(sft_4, Collections.singletonList(f4)));
                                     }
                                     dwd_live_features_4.clear();
                                 
                          // same is here
                          // write these simple features to the kafka topic WIND
                             
                                 System.out.println("Writing " + dwd_live_features_5.size()
                                         + " features to kafka topic WIND2");
                                 for (SimpleFeature f5 : dwd_live_features_5) {
                                     producerFS_5.addFeatures(new ListFeatureCollection(sft_5, Collections.singletonList(f5)));
                                 }
                                 dwd_live_features_5.clear();                                                    
                          // same is here
                          // write these simple features to the kafka topic EXTREME_WIND
                             
                                 System.out.println("Writing " + dwd_live_features_6.size()
                                         + " features to kafka topic EXTREME_WIND");
                                 for (SimpleFeature f6 : dwd_live_features_6) {
                                     producerFS_6.addFeatures(new ListFeatureCollection(sft_6, Collections.singletonList(f6)));
                                 }
                                 dwd_live_features_6.clear();                        
                                    
                return "success";
                }
            		catch (Exception e){
                	return "fail";
                }
            }
           }

            @Override
            public void close() {
                try {

                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        });
         System.out.println("The Application is running successfully");
         // change the name of the proj
        env.execute("dlr.ts.real5G.com.HiwiProject.FlinkApp"); 
    }
}

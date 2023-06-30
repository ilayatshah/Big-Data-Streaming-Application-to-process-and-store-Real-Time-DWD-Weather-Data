from kafka import KafkaProducer
import ftplib
import pandas as pd
import zipfile
import datetime
import os
import json
import glob
import ssl
import http.client
from prometheus_client import Counter, start_http_server
import logging

from prometheus_client import Counter, start_http_server, Histogram, Summary
import urllib.request
import logging
import os
import threading
import time

# paths (where the certificates are mounted as Kubernetes secrets)
current_directory = os.path.dirname(os.path.abspath(__file__))
cert_file = os.path.join(current_directory, 'certs', 'dlrts1008.pem')
key_file = os.path.join(current_directory, 'certs', 'dlrts1008.key')

#### make kafka server and topic configurable ####

# These lines read the environment variables KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC, and if they are not set 
# it uses the default values 'localhost:9092' and 'dwd_extreme_wind_10m', respectively
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'dwd_extreme_wind_10m')

#  add an environment variable to decide which method to use
KAFKA_METHOD = os.environ.get('KAFKA_METHOD', 'rest')

# create a function for the native Kafka producer
def native_kafka_producer(json_string):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol='SSL',
        ssl_cafile=cert_file,
        ssl_certfile=cert_file,
        ssl_keyfile=key_file,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        producer.send(KAFKA_TOPIC, json_string)
        producer.flush()
        kafka_messages_sent.inc()
    except Exception as e:
        logger.error("Error: " + str(e))
        kafka_send_failures.inc()

def rest_kafka_producer(omMsg):
    # get PASSWORD from environment variable
    password = os.environ.get('PASSWORD')
    try:
        CONST_SERVER_URL_EIP = "bahnserver.ts.dlr.de"
        headers = {"Content-Type" : "application/x-www-form-urlencoded", "Connection":"keep-alive"}

        # use the secret information in your code
        sslContext = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        sslContext.check_hostname = False 
        sslContext.load_cert_chain(cert_file, keyfile=key_file, password=password)
        sslContext.verify_mode = ssl.CERT_NONE

        connection = http.client.HTTPSConnection(CONST_SERVER_URL_EIP, 9525, timeout=10, context=sslContext)
        connection.set_debuglevel(1)

        connection.request("POST", "/irmi/applikationen/kafka-producer/kafka.php?TOPIC=dwd_extreme_wind_10m&VERSION=2.0", omMsg, headers)
        response = connection.getresponse()
        responseStr = response.read().strip().decode("utf-8")

        logger.info("\n\n\nResponse:\n")
        logger.info(responseStr)

        # Increment the counter after successfully sending a message
        kafka_messages_sent.inc()

    except Exception as e:
        logger.error("Error: " + str(e))
        kafka_send_failures.inc()

# Setup global proxy handler for urllib
http_proxy = os.environ.get('http_proxy')
https_proxy = os.environ.get('https_proxy')

proxy_support = urllib.request.ProxyHandler({'http': http_proxy, 'https': https_proxy})
opener = urllib.request.build_opener(proxy_support)
urllib.request.install_opener(opener)

# Define the metrics
kafka_messages_sent = Counter('kafka_messages_sent_total', 'Total number of Kafka messages sent')
kafka_message_size = Histogram('kafka_message_size_bytes', 'Size of Kafka messages sent in bytes')
kafka_send_duration = Summary('kafka_send_duration_seconds', 'Time spent sending messages to Kafka in seconds')
kafka_send_failures = Counter('kafka_send_failures_total', 'Number of failed attempts to send messages to Kafka')
kafka_processing_duration = Summary('kafka_processing_duration_seconds', 'Time spent processing messages before sending to Kafka in seconds')
kafka_messages_received_total = Counter('kafka_messages_received_total', 'Total number of Kafka messages received')

# Log level
if 'LOG_LEVEL' in os.environ:
    LOG_LEVEL = logging.getLevelName(os.environ['LOG_LEVEL'])
else:
    LOG_LEVEL = logging.DEBUG

logging.basicConfig(
    filename='extreme_wind.log',
    format='%(asctime)s %(name)s %(levelname)s:%(message)s',
    level=LOG_LEVEL
)

# Create console handler with the same log level and formatter
console_handler = logging.StreamHandler()
console_handler.setLevel(LOG_LEVEL)
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s:%(message)s')
console_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(console_handler)  # Add console handler to the logger


def extreme_wind():       
        # dwd website, which is an open source platform for working with weather data
        logger.info("Connecting to the FTP server")
        ftp = ftplib.FTP("opendata.dwd.de")
        ftp.login()

        # directory on the website where the precipitation files are located 
        ftp.cwd('climate_environment/CDC/observations_germany/climate/10_minutes/extreme_wind/now/')          
        logger.info("Connected to the FTP server")
                
        count = len(ftp.nlst())
        curr = 0
        logger.info("found {} files".format(count))
        
        # if the specified file found in the directory, download them 
        for filename in ftp.nlst():
            #if filename.startswith("10minutenwerte_extrema_wind_000"): # downlowd 2 or 3 Stations data only
            if filename.startswith("10minutenwerte_extrema_wind_"): # download all stations data in Germany
                curr += 1
                logger.info(f"Downloading file {curr}")
                ftp.retrbinary('RETR ' + filename, open(filename, 'wb').write)   
                
        ftp.quit() # close the ftp connection        
        for filename in os.listdir(): # get the list of files
            if zipfile.is_zipfile(filename): # if it is a zipfile, extract it
                with zipfile.ZipFile(filename) as item: # treat the file as a zip
                    item.extractall() # extract it in the working directory
        
        #  put all the .txt files in the folder into files, so that we can append it to a single file in the next step
        files = glob.glob("*.txt")
        
        df_list = [] # create an empty list
        
        for filename in sorted(files):
            df_list.append(pd.read_table(filename, encoding = 'ISO-8859-1', 
                                         sep=";")) #before
        
        #number of data files being processed
        logger.info(f"Processing {len(df_list)} data files")
        # concatinate the list of all the df files into a single df, i.e. df_1
        print("DataFrames:", df_list)

        df_1 = pd.concat(df_list)
        
        df_1['MESS_DATUM'] = pd.DataFrame(df_1['MESS_DATUM'])

        # drop NA values from the MESS_DATUM column if there is any
        df_1 = df_1.dropna(subset=['MESS_DATUM'])

        # Apply the lambda function to convert the date time column, i.e. "MESS_DATUM" into a timestamp
        df_1['MESS_DATUM'] = df_1['MESS_DATUM'].apply(lambda x: datetime.datetime.strptime(str(int(x)),'%Y%m%d%H%M').timestamp())
        
        # we can also create a function in the beginning of this program to do this task again here
        logger.info("connecting again to the FTP server")
        ftp = ftplib.FTP("opendata.dwd.de")
        ftp.login()
        
        ftp.cwd('climate_environment/CDC/observations_germany/climate/10_minutes/extreme_wind/now/')          
        logger.info("Connected successfully")
        count = len(ftp.nlst())
        curr = 0
        
        # download the file .txt file in which we have lat/long of all the stations in Germany
        # later we will extract these lat/long from this file into the data file (df_1) so that we can show the locations of each station
        for filename in ftp.nlst():
            if filename.startswith("zehn_now_fx_Beschreibung_"): 
                curr += 1
                logger.info(f"Downloading lat/long file {filename}")
                ftp.retrbinary('RETR ' + filename, open(filename, 'wb').write)
        ftp.quit()
        logger.info("Completed! As successfully downloaded all the files!")
        
        # because of having unknown/uneven number of columns we have added these extra column names to avoid errors
        names= ['Stations_id', 'von_datum', 'bis_datum', 'Stationshoehe', 
                'geoBreite', 'geoLaenge', 'a', 'b','c','d', 'e']
        
        # convert the downloaded .txt file which is comprised of lat/long of stations into a df (df_2) to fetch the values (lat/long) from it
        df_2 = pd.read_table("zehn_now_fx_Beschreibung_Stationen.txt", 
        skipinitialspace = True, sep=(" +"), engine='python', names=names, encoding=("ISO-8859-1"))
        df_2 = df_2.drop([0,1])
        
        # delete all the .txt files from the dir
        os.chdir(os.getcwd())
        files=glob.glob('*.txt')
        for filename in files:
            os.unlink(filename)
              
        # put all the .zip file extension files in the directory into files2 to delete them after using    
        files2 = glob.glob('*.zip')
        for filename in files2:
            os.unlink(filename)
        logger.info("Deleting downloaded .txt and .zip files")

        
        # convert data type of 'Stations_id' column in the df from obj to an int so that we can join df_1 and df_2 having similar data type
        df_2['Stations_id'] = df_2['Stations_id'].astype(int)
        df_2.rename(columns = {'Stations_id':'STATIONS_ID'}, inplace = True)
        
        # Concatenating data frames
        with kafka_processing_duration.time():
            df_1 = pd.concat(df_list)

            df_1 = df_1.join(df_2.set_index('STATIONS_ID')[['geoBreite','geoLaenge']], on='STATIONS_ID')
            logger.info(f"Joining data frames. Resulting data frame has {len(df_1)} rows")

            # this function convert df to geojson
        def df_to_geojson(df, properties, lat='geoBreite', lon='geoLaenge'):
            geojson = {'type':'FeatureCollection', 'features':[]}
            for _, row in df.iterrows():
                feature = {'type':'Feature',
                        'properties':{},
                        'geometry':{'type':'Point',
                                    'coordinates':[]}}
                feature['geometry']['coordinates'] = [row[lon],row[lat]]
                for prop in properties:
                    feature['properties'][prop] = row[prop]
                geojson['features'].append(feature)
            return geojson
        
        logger.info("df_to_geojson has been converted")    
        
        # pass the function a DataFrame, a list of columns to convert to GeoJSON feature properties
        columns = ['STATIONS_ID', 'MESS_DATUM', 'FX_10', 'FNX_10', 
                   'FMX_10', 'DX_10', 'eor', 'geoBreite', 'geoLaenge']
        
        # add the columns we specified above in data frame (df_1) and convert it to geojson  
        geojson = df_to_geojson(df_1, columns)
        
        # convert the geojson to a json string so that we can send it to the kafka topic later
        json_string = json.dumps(geojson)

        ### using this we can send the message to the Kafka topic using either the native Kafka producer 
        # or the REST interface, depending on the value of the KAFKA_METHOD environment variable.
        with kafka_send_duration.time():
            if KAFKA_METHOD == 'native':
                native_kafka_producer(json_string)
            else:
                rest_kafka_producer(json_string)
        kafka_message_size.observe(len(json_string))    

def run_prometheus_server():
    start_http_server(8000)
    logger.info("Prometheus server started on port 8000")


if __name__ == '__main__':
    prometheus_thread = threading.Thread(target=run_prometheus_server)
    prometheus_thread.start()
    extreme_wind()

# unable to see the metrics without the loop, and adding the loop made the metrics visible.
# The reason for this is likely due to the way the main thread of our script is terminating 
# before the HTTP server has a chance to serve the metrics. Therefore, add the loop 
# to see the matrics
while True:
    time.sleep(900)  # Or any other duration in seconds
    extreme_wind()


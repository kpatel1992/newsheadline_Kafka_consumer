# This is the Kafka consumer for the data is streaming through a Kafka topic, the challenge consists in tapping onto that topic, 
# 
# Decoding the headlines and extracting from the first 1000 records the ones that contain the word australia. 
# Search the headlines with australia in encoded form ".- ..- ... - .-. .- .-.. .. .-/", NOT australian or australians and extracted into file.
# 
# Save the extraction into an output text file name newsheadlines_first_1000.txt with one headline per line, decoded back to English.
# 
# For decode back to english used the api mentioned in the challenge.
# 
# To consume and read/process message, I have used Confluent-kafka is a high-performance Kafka client for Python 
# which leverages the high-performance C client librdkafka
#
# The message's processing is asynchronous and I have used asynchronous commit approach to manually commit for apache offset management instead of auto commit.
#

from confluent_kafka import Consumer,KafkaError
import re
import time
import os
import requests

# Create Consumer instance
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'morse-code-local-0',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'session.timeout.ms': 6000,
    'fetch.min.bytes': 100000
})

# Dictionary representing the morse code chart
MORSE_CODE_DICT = { 'A':'.-', 'B':'-...',
                    'C':'-.-.', 'D':'-..', 'E':'.',
                    'F':'..-.', 'G':'--.', 'H':'....',
                    'I':'..', 'J':'.---', 'K':'-.-',
                    'L':'.-..', 'M':'--', 'N':'-.',
                    'O':'---', 'P':'.--.', 'Q':'--.-',
                    'R':'.-.', 'S':'...', 'T':'-',
                    'U':'..-', 'V':'...-', 'W':'.--',
                    'X':'-..-', 'Y':'-.--', 'Z':'--..',
                    '1':'.----', '2':'..---', '3':'...--',
                    '4':'....-', '5':'.....', '6':'-....',
                    '7':'--...', '8':'---..', '9':'----.',
                    '0':'-----', ', ':'--..--', '.':'.-.-.-',
                    '?':'..--..', '/':'-..-.', '-':'-....-',
                    '(':'-.--.', ')':'-.--.-'}
# api-endpoint
URL = "http://localhost:8083/kafka-coding-challenge/translate?morse-code="

# Function to decrypt the string
# from morse to english
def decrypt(message):
 
    # extra space added at the end to access the
    # last morse code
    message += ' '
 
    decipher = ''
    citext = ''
    for letter in message:
 
        # checks for space
        if (letter != ' '):
 
            # counter to keep track of space
            i = 0
 
            # storing morse code of a single character
            citext += letter
 
        # in case of space
        else:
            # if i = 1 that indicates a new character
            i += 1
 
            # if i = 2 that indicates a new word
            if i == 2 :
 
                 # adding space to separate words
                decipher += ' '
            else:
 
                # reverse of encryption via API call
                response = requests.get(url = URL+citext)
                decipher += response.text
                #print(decipher)
                citext = ''
	
    return decipher

# Function to consume poll and process messages
def consume_loop(consumer, topics):
    try:
        # consumer subscribe to topic
        consumer.subscribe(topics)
        # Minimum Consumer commit count
        MIN_COMMIT_COUNT = 100
        # Initialise Buffer List
        msg_buffer = []
        # Set Minimum buffer list size
        buffer_size = 100
        # Initialise the message count
        msg_count = 0
        # Initialise the record reading count
        read_count=0
                
        while True:
            # Process messages first 1000 records the ones that contain the word australia else stop reading
            if read_count<1000:
                
                # Poll message from consumer
                msg = consumer.poll(timeout=1.0)
                                            
                # Message is None then continue for next message
                if msg is None: continue
                
                # Message having error then print that error and read the next message
                if msg.error():
                    
                    # Message error regarding kafka partition end of file then print the details else raise kafka exeception
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                
                    if msg_count ==0:
                        print("Topic Message Start offset value is : {}.".format(msg.offset()))
            
                                        
                    # Search in message contain stricly australia keyword in headline then add in list
                    if re.search(r'/.- ..- ... - .-. .- .-.. .. .-/', msg.value().decode('utf-8')) or re.search(r'.- ..- ... - .-. .- .-.. .. .-/', msg.value().decode('utf-8')):

                        #Read the message and decrypt the message
                        result = decrypt(msg.value().decode('utf-8').replace('/','  ')).lower()

                        read_count += 1  # counter to keep track of extract records
                        msg_buffer.append(result) # add record into buffer list
                        
                        # Once buffer list is reach to buffer_size then the data will be added into text file.
                        if len(msg_buffer) >= buffer_size:
                        
                            print("Max Buffer Size reached,Writing data into File.")
                            
                            #Append the data into text file
                            with open('newsheadlines_first_1000.txt', 'a') as f:
                                f.writelines("%s\n" % newsheadlines for newsheadlines in msg_buffer)
                                
                            msg_buffer = [] # Reset the buffer list
                            
                            print("Total Number of records extract with australia keyword is : {} from total Number of records reads is : {}.".format(read_count,msg_count))
                            print("Topic Message current offset value is : {}.".format(msg.offset()))
                        
                    msg_count += 1 # counter to keep track of message read records
                    
                    # Asynchronous commit once message read count reach at MIN_COMMIT_COUNT
                    if msg_count % MIN_COMMIT_COUNT == 0:
                        #print("Asynchronous commit after number of message read, {} ".format(msg_count))
                        consumer.commit(asynchronous=True)
            else:
                break
    except KeyboardInterrupt:
        pass
    finally:
        print("Leave group and commit final offsets and write the records in file")
            
        # Close down consumer to commit final offsets.
        consumer.close()     

if __name__ == '__main__':
    
    starttime = time.localtime() # get struct_time
    starttime_string = time.strftime("%m/%d/%Y, %H:%M:%S", starttime)
    print("Consumer Started at:{}".format(starttime_string))
    
    # Delete file if exist
    if os.path.exists("newsheadlines_first_1000.txt"):
        print("File Exist and Delete the file before read and write.")
        os.remove("newsheadlines_first_1000.txt")
    
    # Consumer consume the data from the topic
    consume_loop(c,['au.com.eliiza.newsheadlines.txt'])
    
    endtime = time.localtime() # get struct_time
    endtime_string = time.strftime("%m/%d/%Y, %H:%M:%S", endtime)
    print("Consumer End at:{}".format(endtime_string))
    
    print("Total Time to Process 1000 australia keyword messages is: {} minutes.".format((time.mktime(endtime) - time.mktime(starttime)) / 60))

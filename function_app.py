
import azure.functions as func
import logging
from azure.storage.blob import BlobClient
import os 
from datetime import datetime, timezone
from azure.storage.blob import ContainerClient


# Create the FunctionApp instance with anonymous access
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.blob_trigger(arg_name="myblob", 
                  path="image-input/{name}",
                  connection="SOURCE_STORAGE_CONNECTION")
@app.blob_output(arg_name="outputblob",
                 path="image-output/mapping-{name}",
                 connection="SOURCE_STORAGE_CONNECTION")
def blob_function(myblob: func.InputStream, outputblob: func.Out[str]):
    # Log blob metadata
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    # Read the binary content of the input blob
    image_data = myblob.read()
    
    # Write the binary content to the output blob
    outputblob.set(image_data)
    
    logging.info(f"Successfully copied the blob to destination storage.")



@app.timer_trigger(arg_name="mytimer", schedule="0 */5 * * * *")  # Every 5 minutes
@app.queue_output(arg_name="outputQueueItem", queue_name="outqueue",
                 connection="AzureWebJobsStorage")
def timer_function(mytimer: func.TimerRequest, outputQueueItem: func.Out[str]):
    # Check if the timer is due to run
    if mytimer.past_due:
        logging.warning("The timer is running late!")
    
    # Log timer information
    logging.info("Timer trigger function executed.")
    
    # Perform periodic tasks here (e.g., health checks, log cleanup, etc.)
    logging.info("Performing scheduled file cleanup.")

    utc_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc)

    logging.info('Python timer trigger function ran at %s', utc_timestamp.isoformat())

    storage_connection_string = os.environ["AzureWebJobsStorage"]
    container = ContainerClient.from_connection_string(conn_str=storage_connection_string, container_name="image-input")
    blob_list = container.list_blobs()
    
    for blob in blob_list:
        
        diff = utc_timestamp - blob.creation_time
        # If the blob is older than X days/minutes/seconds, delete it

        if(diff.seconds > 120):
            blob_client = container.get_blob_client(blob)
            blob_client.delete_blob()
    
            # Store it in the queue output binding
            outputQueueItem.set(blob.name)



@app.queue_trigger(arg_name="msg", queue_name="inputqueue",
                   connection="AzureWebJobsStorage")  # Queue trigger
def test_function(msg: func.QueueMessage,
                  ) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))
    
    


























import json
import time
import boto3
import logging
import json

def lambda_handler(event, context):
    # TODO implement
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    DATA_BASE = 'data-lake-customer-poc-db'
    BUCKET_NAME = 'data-lake-us-west-2-924175341144' 
    S3_OUTPUT = 's3://data-lake-us-west-2-924175341144-two-tables-join'
    
    #logger.info("event " + repr(event.get("queryStringParameters").get("Serial_Number")))
    
    #retrieve request parameter - serial number
    #serial_number = event.get("queryStringParameters").get("Serial_Number")
    
    #for local test
    serial_number = event.get("Serial_Number")
    #number of retries
    RETRY_COUNT = 20
    logger.info("serial number = " + repr(serial_number))
    
    #Query string to fetch order number.   #'S12612523509051'
    
    
    #query against csv tables.
    
    #query = "SELECT DISTINCT cb.name FROM gp_system s, gp_serial_number sn, gp_customer_rack_to_system crs, gp_customer_build_to_system cbs, "\
    #"gp_customer_build cb, gp_customer_rack cr WHERE cr.build_id = cb.id AND sn.id = s.serial_id AND s.id = crs.system_id "\
    #"AND cr.id = crs.rack_id AND sn.number = " + "'" + str(serial_number) + "'"   
    
    
    
    #query against parquet tables.
    query = "SELECT DISTINCT cb.name FROM system s, serial_number sn, customer_rack_to_system crs, customer_build_to_system cbs, customer_build cb, "\
    "customer_rack cr WHERE sn.id = s.serial_id AND s.id = crs.system_id AND cr.id = crs.rack_id AND cr.build_id = cb.id AND sn.number = "  + "'" + str(serial_number) + "'" 
    
    
    #    query = "SELECT DISTINCT cb.col1 FROM gp_system s, gp_serial_number sn, gp_customer_rack_to_system crs, "\
    #    "gp_customer_build_to_system cbs, gp_customer_build cb, gp_customer_rack cr "\
    #    "WHERE sn.col0 = s.col4 AND s.col0 = crs.col2 "\
    #    "AND cr.col0 = crs.col1 AND cr.col5 = cb.col0 AND sn.col1 = " + "'" + str(serial_number) + "'"
    
    client = boto3.client('athena')
    logger.info(" before execution")
    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATA_BASE
        },
        ResultConfiguration={
            'OutputLocation': S3_OUTPUT,
        }
    )
    # get query execution id
    query_execution_id = response['QueryExecutionId']
    logger.info("query id = " + query_execution_id )
        # get execution status
    for i in range(1, 1 + RETRY_COUNT):

        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')
    
    
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    logger.info("after execution  " + repr(result))
    val1 = result.get("ResultSet").get("Rows")[1].get("Data")[0].get("VarCharValue")
   
    logger.info("order number = " + val1)

    return {
        "statusCode": 200,
        "body": json.dumps('For the given serial Number: ' +  str(serial_number) +    ', the  Order Number: ' + val1 +' is returned!')
    }

import test

this is test

second test

third test

4iiourth test

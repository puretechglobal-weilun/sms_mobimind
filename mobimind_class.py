import boto3
import uuid
import json
import random
from boto3.dynamodb.conditions import Key

def insert_mo(function_json):
    original_request_json = {}
    dynamoDB_status = ""
    for key, value in function_json.items():
        original_request_json[key] = value
    transaction_id = original_request_json.get("transaction_id", "")
    mo_id = original_request_json.get("mo_id", "")
    rid = original_request_json["country"]+"_"+original_request_json["gateway"]+"_"+original_request_json["operator"]+"_"+original_request_json["shortcode"]+"_"+original_request_json["keyword"]+"_"+original_request_json["msisdn"]
    rid = rid+"_"+mo_id if mo_id else rid
    mo_message = generate_mo_message(original_request_json["keyword"], original_request_json["status"], transaction_id)
    mo_type = mo_parser_message(mo_message)
    mo_type = json.loads(mo_type)
    mo_type = mo_type["type"]
    insert_params = {
        "subscriber_id": rid,
        "date_time": original_request_json.get("global_datetime", "0000-00-00 00:00:00"),
        "country": original_request_json.get("country", ""),
        "gateway": original_request_json.get("gateway", ""),
        "operator": original_request_json.get("operator", ""),
        "shortcode": original_request_json.get("shortcode", ""),
        "keyword": original_request_json.get("keyword", ""),
        "msisdn": original_request_json.get("msisdn", ""),
        "mo_id": original_request_json.get("mo_id", ""),
        "mo_message": mo_message,
        "mo_message_raw": "STATUS="+original_request_json.get("status", ""),
        "mo_type": mo_type,
        "process_status": "processing"
    }
    return json.dumps(insert_params)
    
def mo_parser_message(mo_message):
    parser_params = {}
    parser_params["type"] = "unknown"
    parser_message = mo_message
    sub_array   = ["subc","on"]
    unsub_array = ["unsub","blocked","stop"]
    split_data  = mo_message.split(" ")
    function    = split_data[0]
    if function in unsub_array:
        parser_params["type"]       = "unsub"
        parser_params["keyword"]    = split_data[1].strip()
        parser_params["seckeyword"] = split_data[2].strip()
        parser_params["mo_message"] = " ".join(parser_params.values()).strip()
        if parser_params["keyword"] == "all" or parser_params["keyword"] == "semua":
            parser_params["type"]       = "unsub all"
            parser_params["keyword"]    = split_data[1].strip()
            parser_params["seckeyword"] = split_data[2].strip()
            parser_params["mo_message"] = " ".join(parser_params.values()).strip()
        elif parser_params["keyword"] == "":
            parser_params["type"]       = "unsub all"
            parser_params["keyword"]    = split_data[1].strip()
            parser_params["seckeyword"] = split_data[2].strip()
            parser_params["mo_message"] = " ".join(parser_params.values()).strip()
    elif function in sub_array:
        parser_params["type"]       = "sub"
        parser_params["keyword"]    = split_data[1].strip()
        parser_params["seckeyword"] = split_data[2].strip()
        parser_params["mo_message"] = " ".join(parser_params.values()).strip()
    elif mo_message != "":
        parser_params["type"]       = "sub"
        parser_params["keyword"]    = split_data[0].strip()
        parser_params["seckeyword"] = split_data[1].strip()
        parser_params["mo_message"] = " ".join(parser_params.values()).strip()
    return json.dumps(parser_params)

def generate_mo_message(keyword, status, transaction_id = ""):
    text = ""
    if status == "act-sb" or status == "subc":
        text = "on "+keyword+" "+transaction_id.strip()

    if (status == "blocked" or status == "bld-sb" or status == "unsub"):
        text = "stop "+keyword+" "+transaction_id.strip()
    return text

def process_subscriber_add_data(function_json):
    original_request_json = {}
    subscriber_data = {}
    for key, value in function_json.items():
        original_request_json[key] = str(value)
    subscriber_data["country"] = original_request_json["country"]
    subscriber_data["gateway"] = original_request_json["gateway"]
    return json.dumps(subscriber_data)
    
def process_send_sms(function_json):
    if function_json["message_key"] == "welcome":
        function_json["mt_message"]     = "hello world testing"
        function_json["mt_price"]       = "0.00"
        function_json["mt_category"]    = "welcome"
        function_json["mt_send"]        = "yes"
        message_body =  json.dumps(function_json)
        sqs = boto3.resource("sqs")
        queue = sqs.get_queue_by_name(QueueName=function_json["gateway"]+"_"+function_json["country"]+"_fast")
        response = queue.send_message(MessageBody=message_body)
        return json.dumps(response)
    elif function_json["message_key"] == "duplicate_subscription":
        return function_json["message_key"]+" do nothing"
    elif function_json["message_key"] == "non_subscriber":
        return function_json["message_key"]+" do nothing"
    elif function_json["message_key"] == "quit_message":
        return function_json["message_key"]+" do nothing"
    elif function_json["message_key"] == "stop_all_message":
        return function_json["message_key"]+" do nothing"

def process_send_content(function_json):
    return "process_send_content trigger done"

def get_keyword(country,gateway,operator,shortcode,keyword):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("keyword")
    product = country + "_" + gateway + "_" + operator + "_" + shortcode + "_" + keyword
    response = table.query(
        KeyConditionExpression=Key("country").eq(str(country)) & Key("product").eq(str(product))
    )
    return list(response["Items"])[0]

def get_subscriber(country,gateway,operator,shortcode,keyword,msisdn):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("subscriber_"+ dn_data["gateway"])
    rid = country + "_" + gateway + "_" + operator + "_" + shortcode + "_" + keyword + "_" + msisdn 
    response = table.query(
        IndexName="msisdn-rid-index",
        KeyConditionExpression=Key("msisdn").eq(str(msisdn)) & Key("rid").begins_with(str(rid))
    )
    return list(response["Items"])[0]

def update_dn(dn_json):
    dn_data = {}
    internal_debug = {}
    
    for key, value in dn_json.items():
        dn_data[key] = str(value)

    keyword_data = {}
    #get keyword details
    keyword_details = get_keyword(dn_data["country"],dn_data["gateway"],dn_data["operator"],dn_data["shortcode"],dn_data["keyword"])
    for key, value in keyword_details.items():
        keyword_data[key] = str(value)
        
    #return "DN UPDATED"
    subscriber_data = {}
    #get subscriber details from elastic cache
    subscriber_details = get_subscriber(dn_data["country"],dn_data["gateway"],dn_data["operator"],dn_data["shortcode"],dn_data["keyword"],dn_data["msisdn"])
    for key, value in subscriber_details.items():
        subscriber_data[key] = str(value)
    
    #update / create MT
    dynamodb = boto3.resource("dynamodb")
    dynamoDB_status = ""
    table = dynamodb.Table( "mt_"+ dn_data["gateway"])
    dynamoDB_status  = table.update_item(
        Key={"mt_id": dn_data["msg_id"],"msisdn": dn_data["msisdn"] },
        UpdateExpression="SET dn_code = :dn_code , mt_code = :mt_code",
        ExpressionAttributeValues={
            ":dn_code": dn_json["status"],
            ":mt_code": "m305"
        },
        ReturnValues="UPDATED_NEW"
    )
    msg_id = dn_data["msg_id"]
    
    #create MT 

    # mt_json={}
    # mt_json["mt_id"] = str(uuid.uuid4())
    # mt_json["msisdn"] = dn_data["msisdn"]
    # mt_json["category"] =  "schedule"
    # mt_json["country"] =  dn_data["country"]
    # mt_json["gateway_code"] =  dn_json["status"]
    # mt_json["keyword"] =  dn_data["keyword"]
    # mt_json["message"] = "testing"
    # mt_json["mt_code"] =  "m306"
    # mt_json["mt_last_update_time"] = dn_data["global_datetime"]
    # mt_json["mt_message_id"] = mt_json["mt_id"]
    # mt_json["mt_sent_time"] = dn_data["global_datetime"]
    # mt_json["operator"] = dn_data["operator"]
    # mt_json["price"] = "0.00"
    # mt_json["shortcode"] =  dn_data["shortcode"]
    # mt_json["subscriber_id"] = subscriber_data["rid"]
    # msg_id = mt_json["mt_id"]
    
    
    # table = dynamodb.Table("mt_"+ dn_data["gateway"])
    # dynamoDB_status = table.put_item(Item=mt_json)
    dynamoDB_status = dynamoDB_status["ResponseMetadata"]["HTTPStatusCode"]
    if dynamoDB_status == 200:
        return msg_id, dn_data["global_datetime"], dynamoDB_status
    else:
        return "DynamoDB got problem"
    
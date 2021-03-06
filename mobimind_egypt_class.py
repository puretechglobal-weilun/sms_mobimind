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
    return insert_params
    
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

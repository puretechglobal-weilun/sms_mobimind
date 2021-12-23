import json
import uuid
import os.path
def handler(event, context):
    params_json = {} #the json for trigger any function
    
    for key, value in event.items(): #for loop all the data and also assign the key value into "params_json"
        params_json[key]    = value
        
    function = params_json["function"]
    
    if params_json["country"] and params_json["gateway"]: #params_json["country"] and params_json["gateway"] is the much
        # assign the default variable 
        trigger = "yes"
        final_class         = ""
        class_file          = __import__(params_json["gateway"]+"_"+"class") #import the default class file
        special_class_file  = params_json["gateway"]+"_"+params_json["country"]+"_"+"class"
        checking            = os.path.exists(special_class_file+".py") 
        
        if checking == True: #check the file is exist or not to avoid error (per params_json["country"],params_json["gateway"] level) 
            special_class_file  = __import__(special_class_file)    
            # special_function    = params_json["gateway"]+"_"+params_json["country"]+"_"+function
            checking = function in dir(special_class_file) 
            if checking == True: # check the function is inside the file 
                final_class     = special_class_file
            else: # run the default file
                checking = function in dir(class_file)
                if checking == True: # check the function is inside the default file 
                    final_class     = class_file
                else:
                    trigger = "no"
        else:
            checking = function in dir(class_file)
            if checking == True: # check the function is inside the default file 
                final_class     = class_file
            else:
                trigger = "no"

        if trigger == "yes":
            function_trigger = getattr(final_class, function)(params_json) #trigger function and return something
        else:
            function_trigger = "skip"

        return {
            ('"%s"' % final_class+function)  :   function_trigger
        }
    else:
        return {
            "status"    :   "country and gateway is must key value",
            "code"      :   400
        }
 
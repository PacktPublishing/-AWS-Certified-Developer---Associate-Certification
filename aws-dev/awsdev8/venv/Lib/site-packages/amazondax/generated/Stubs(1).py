# 
#  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# 
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not 
#  use this file except in compliance with the License. A copy of the License 
#  is located at
# 
#     http://aws.amazon.com/apache2.0/
# 
#  or in the "license" file accompanying this file. This file is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
#  express or implied. See the License for the specific language governing 
#  permissions and limitations under the License.
#
from .. import Constants, AttributeValueEncoder





def write_authorizeConnection_1489122155_1(accessKeyId, signature, stringToSign, sessionToken, userAgent, tube):
    tube.write_int(1)
    tube.write_int(1489122155)
    tube.write_string(accessKeyId)
    
    tube.write_string(signature)
    
    tube.write_binary(stringToSign)
    
    if sessionToken is None:
        tube.write_null()
    else:
        tube.write_string(sessionToken)
    
    if userAgent is None:
        tube.write_null()
    else:
        tube.write_string(userAgent)
    

    tube.flush()

def write_batchGetItem_N697851100_1(request, tube):
    tube.write_int(1)
    tube.write_int(-697851100)
    AttributeValueEncoder.encode_batchGetItem_N697851100_1_getItemKeys_direct(tube.enc, request)
    
    
    has_kwargs = (("ReturnConsumedCapacity" in request))
    if has_kwargs:
        tube.enc.append_map_stream_header()
        if 'ReturnConsumedCapacity' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnConsumedCapacity)
            tube.write_int(getattr(Constants.ReturnConsumedCapacityValues, request.ReturnConsumedCapacity.upper()))
    
    
        tube.enc.append_break()
    else:
        tube.write_null()

    tube.flush()

def write_batchWriteItem_116217951_1(request, tube):
    tube.write_int(1)
    tube.write_int(116217951)
    AttributeValueEncoder.encode_batchWriteItem_116217951_1_keyValuesByTable_direct(tube.enc, request)
    
    
    has_kwargs = (("ReturnConsumedCapacity" in request) or 
            ("ReturnItemCollectionMetrics" in request))
    if has_kwargs:
        tube.enc.append_map_stream_header()
        if 'ReturnConsumedCapacity' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnConsumedCapacity)
            tube.write_int(getattr(Constants.ReturnConsumedCapacityValues, request.ReturnConsumedCapacity.upper()))
    
        if 'ReturnItemCollectionMetrics' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnItemCollectionMetrics)
            tube.write_int(getattr(Constants.ReturnItemCollectionMetricsValues, request.ReturnItemCollectionMetrics.upper()))
    
    
        tube.enc.append_break()
    else:
        tube.write_null()

    tube.flush()

def write_defineAttributeList_670678385_1(attributeListId, tube):
    tube.write_int(1)
    tube.write_int(670678385)
    tube.write_int(attributeListId)
    

    tube.flush()

def write_defineAttributeListId_N1230579644_1(attributeNames, tube):
    tube.write_int(1)
    tube.write_int(-1230579644)
    tube.write_array(attributeNames)
    

    tube.flush()

def write_defineKeySchema_N742646399_1(tableName, tube):
    tube.write_int(1)
    tube.write_int(-742646399)
    tube.write_binary(tableName)
    

    tube.flush()

def write_deleteItem_1013539361_1(request, tube):
    tube.write_int(1)
    tube.write_int(1013539361)
    tube.write_binary(request.TableName)
    tube.write_binary(AttributeValueEncoder.encode_key(request.Key, request._key_schema))
    
    has_kwargs = (("ReturnValues" in request) or 
            ("ReturnConsumedCapacity" in request) or 
            ("ReturnItemCollectionMetrics" in request) or 
            ("ConditionExpression" in request) or 
            ("ExpressionAttributeNames" in request) or 
            ("ExpressionAttributeValues" in request))
    if has_kwargs:
        tube.enc.append_map_stream_header()
        if 'ReturnValues' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnValues)
            tube.write_int(getattr(Constants.ReturnValuesValues, request.ReturnValues.upper()))
    
        if 'ReturnConsumedCapacity' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnConsumedCapacity)
            tube.write_int(getattr(Constants.ReturnConsumedCapacityValues, request.ReturnConsumedCapacity.upper()))
    
        if 'ReturnItemCollectionMetrics' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnItemCollectionMetrics)
            tube.write_int(getattr(Constants.ReturnItemCollectionMetricsValues, request.ReturnItemCollectionMetrics.upper()))
    
        if 'ConditionExpression' in request:
            tube.write_int(Constants.DaxDataRequestParam.ConditionExpression)
            # kwargs strings are always written as UTF-8 encoded binary types
            tube.write_binary(request.ConditionExpression.encode('utf8'))
    
        # This operation has expressions, so deal with those together
        AttributeValueEncoder.encode_expressions_direct(tube.enc, request)
    
        tube.enc.append_break()
    else:
        tube.write_null()

    tube.flush()

def write_endpoints_455855874_1(tube):
    tube.write_int(1)
    tube.write_int(455855874)

    tube.flush()

def write_getItem_263244906_1(request, tube):
    tube.write_int(1)
    tube.write_int(263244906)
    tube.write_binary(request.TableName)
    tube.write_binary(AttributeValueEncoder.encode_key(request.Key, request._key_schema))
    
    has_kwargs = (("ConsistentRead" in request) or 
            ("ReturnConsumedCapacity" in request) or 
            ("ProjectionExpression" in request) or 
            ("ExpressionAttributeNames" in request))
    if has_kwargs:
        tube.enc.append_map_stream_header()
        if 'ConsistentRead' in request:
            tube.write_int(Constants.DaxDataRequestParam.ConsistentRead)
            tube.write_boolean(request.ConsistentRead)
    
        if 'ReturnConsumedCapacity' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnConsumedCapacity)
            tube.write_int(getattr(Constants.ReturnConsumedCapacityValues, request.ReturnConsumedCapacity.upper()))
    
        # This operation has expressions, so deal with those together
        AttributeValueEncoder.encode_expressions_direct(tube.enc, request)
    
        tube.enc.append_break()
    else:
        tube.write_null()

    tube.flush()

def write_putItem_N2106490455_1(request, tube):
    tube.write_int(1)
    tube.write_int(-2106490455)
    tube.write_binary(request.TableName)
    tube.write_binary(AttributeValueEncoder.encode_key(request.Item, request._key_schema))
    tube.write_binary(AttributeValueEncoder.encode_values(request.Item, request._key_schema, request._attr_names, request._attr_list_id));
    
    has_kwargs = (("ReturnValues" in request) or 
            ("ReturnConsumedCapacity" in request) or 
            ("ReturnItemCollectionMetrics" in request) or 
            ("ConditionExpression" in request) or 
            ("ExpressionAttributeNames" in request) or 
            ("ExpressionAttributeValues" in request))
    if has_kwargs:
        tube.enc.append_map_stream_header()
        if 'ReturnValues' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnValues)
            tube.write_int(getattr(Constants.ReturnValuesValues, request.ReturnValues.upper()))
    
        if 'ReturnConsumedCapacity' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnConsumedCapacity)
            tube.write_int(getattr(Constants.ReturnConsumedCapacityValues, request.ReturnConsumedCapacity.upper()))
    
        if 'ReturnItemCollectionMetrics' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnItemCollectionMetrics)
            tube.write_int(getattr(Constants.ReturnItemCollectionMetricsValues, request.ReturnItemCollectionMetrics.upper()))
    
        if 'ConditionExpression' in request:
            tube.write_int(Constants.DaxDataRequestParam.ConditionExpression)
            # kwargs strings are always written as UTF-8 encoded binary types
            tube.write_binary(request.ConditionExpression.encode('utf8'))
    
        # This operation has expressions, so deal with those together
        AttributeValueEncoder.encode_expressions_direct(tube.enc, request)
    
        tube.enc.append_break()
    else:
        tube.write_null()

    tube.flush()

def write_query_N931250863_1(request, tube):
    tube.write_int(1)
    tube.write_int(-931250863)
    tube.write_binary(request.TableName)
    expressions = AttributeValueEncoder.encode_key_condition_expression_direct(tube.enc, request)
    
    has_kwargs = (("IndexName" in request) or 
            ("Select" in request) or 
            ("Limit" in request) or 
            ("ConsistentRead" in request) or 
            ("ScanIndexForward" in request) or 
            ("ExclusiveStartKey" in request) or 
            ("ReturnConsumedCapacity" in request) or 
            ("ProjectionExpression" in request) or 
            ("FilterExpression" in request) or 
            ("ExpressionAttributeNames" in request) or 
            ("ExpressionAttributeValues" in request))
    if has_kwargs:
        tube.enc.append_map_stream_header()
        if 'IndexName' in request:
            tube.write_int(Constants.DaxDataRequestParam.IndexName)
            # kwargs strings are always written as UTF-8 encoded binary types
            tube.write_binary(request.IndexName.encode('utf8'))
    
        if 'Select' in request:
            tube.write_int(Constants.DaxDataRequestParam.Select)
            tube.write_int(getattr(Constants.SelectValues, request.Select.upper()))
    
        if 'Limit' in request:
            tube.write_int(Constants.DaxDataRequestParam.Limit)
            tube.write_int(request.Limit)
    
        if 'ConsistentRead' in request:
            tube.write_int(Constants.DaxDataRequestParam.ConsistentRead)
            tube.write_int(int(request.ConsistentRead))
    
        if 'ScanIndexForward' in request:
            tube.write_int(Constants.DaxDataRequestParam.ScanIndexForward)
            tube.write_int(int(request.ScanIndexForward))
    
        if 'ExclusiveStartKey' in request:
            tube.write_int(Constants.DaxDataRequestParam.ExclusiveStartKey)
            # No encoder for map so use custom encoder
            AttributeValueEncoder.encode_ExclusiveStartKey_direct(tube.enc, request)
    
        if 'ReturnConsumedCapacity' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnConsumedCapacity)
            tube.write_int(getattr(Constants.ReturnConsumedCapacityValues, request.ReturnConsumedCapacity.upper()))
    
        # This operation has expressions, so deal with those together
        # For Query, the expressions are already eval'd for KeyCondExpr
        AttributeValueEncoder.write_expressions_direct(tube.enc, expressions, request)
    
        tube.enc.append_break()
    else:
        tube.write_null()

    tube.flush()

def write_scan_N1875390620_1(request, tube):
    tube.write_int(1)
    tube.write_int(-1875390620)
    tube.write_binary(request.TableName)
    
    has_kwargs = (("IndexName" in request) or 
            ("Limit" in request) or 
            ("Select" in request) or 
            ("ExclusiveStartKey" in request) or 
            ("ReturnConsumedCapacity" in request) or 
            ("TotalSegments" in request) or 
            ("Segment" in request) or 
            ("ProjectionExpression" in request) or 
            ("FilterExpression" in request) or 
            ("ExpressionAttributeNames" in request) or 
            ("ExpressionAttributeValues" in request) or 
            ("ConsistentRead" in request))
    if has_kwargs:
        tube.enc.append_map_stream_header()
        if 'IndexName' in request:
            tube.write_int(Constants.DaxDataRequestParam.IndexName)
            # kwargs strings are always written as UTF-8 encoded binary types
            tube.write_binary(request.IndexName.encode('utf8'))
    
        if 'Limit' in request:
            tube.write_int(Constants.DaxDataRequestParam.Limit)
            tube.write_int(request.Limit)
    
        if 'Select' in request:
            tube.write_int(Constants.DaxDataRequestParam.Select)
            tube.write_int(getattr(Constants.SelectValues, request.Select.upper()))
    
        if 'ExclusiveStartKey' in request:
            tube.write_int(Constants.DaxDataRequestParam.ExclusiveStartKey)
            # No encoder for map so use custom encoder
            AttributeValueEncoder.encode_ExclusiveStartKey_direct(tube.enc, request)
    
        if 'ReturnConsumedCapacity' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnConsumedCapacity)
            tube.write_int(getattr(Constants.ReturnConsumedCapacityValues, request.ReturnConsumedCapacity.upper()))
    
        if 'TotalSegments' in request:
            tube.write_int(Constants.DaxDataRequestParam.TotalSegments)
            tube.write_int(request.TotalSegments)
    
        if 'Segment' in request:
            tube.write_int(Constants.DaxDataRequestParam.Segment)
            tube.write_int(request.Segment)
    
        if 'ConsistentRead' in request:
            tube.write_int(Constants.DaxDataRequestParam.ConsistentRead)
            tube.write_int(int(request.ConsistentRead))
    
        # This operation has expressions, so deal with those together
        AttributeValueEncoder.encode_expressions_direct(tube.enc, request)
    
        tube.enc.append_break()
    else:
        tube.write_null()

    tube.flush()

def write_updateItem_1425579023_1(request, tube):
    tube.write_int(1)
    tube.write_int(1425579023)
    tube.write_binary(request.TableName)
    tube.write_binary(AttributeValueEncoder.encode_key(request.Key, request._key_schema))
    
    has_kwargs = (("ReturnValues" in request) or 
            ("ReturnConsumedCapacity" in request) or 
            ("ReturnItemCollectionMetrics" in request) or 
            ("UpdateExpression" in request) or 
            ("ConditionExpression" in request) or 
            ("ExpressionAttributeNames" in request) or 
            ("ExpressionAttributeValues" in request))
    if has_kwargs:
        tube.enc.append_map_stream_header()
        if 'ReturnValues' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnValues)
            tube.write_int(getattr(Constants.ReturnValuesValues, request.ReturnValues.upper()))
    
        if 'ReturnConsumedCapacity' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnConsumedCapacity)
            tube.write_int(getattr(Constants.ReturnConsumedCapacityValues, request.ReturnConsumedCapacity.upper()))
    
        if 'ReturnItemCollectionMetrics' in request:
            tube.write_int(Constants.DaxDataRequestParam.ReturnItemCollectionMetrics)
            tube.write_int(getattr(Constants.ReturnItemCollectionMetricsValues, request.ReturnItemCollectionMetrics.upper()))
    
        if 'ConditionExpression' in request:
            tube.write_int(Constants.DaxDataRequestParam.ConditionExpression)
            # kwargs strings are always written as UTF-8 encoded binary types
            tube.write_binary(request.ConditionExpression.encode('utf8'))
    
        # This operation has expressions, so deal with those together
        AttributeValueEncoder.encode_expressions_direct(tube.enc, request)
    
        tube.enc.append_break()
    else:
        tube.write_null()

    tube.flush()


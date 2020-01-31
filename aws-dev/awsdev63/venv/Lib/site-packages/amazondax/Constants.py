# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License
# is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

import decimal
import six

STRING_TYPES = (six.text_type,)
BINARY_TYPES = (bytes, bytearray)
INTEGRAL_TYPES = six.integer_types
REAL_TYPES = (float, decimal.Decimal)
NUMBER_TYPES = INTEGRAL_TYPES + REAL_TYPES
SEQ_TYPES = (list, tuple)

class DaxMethodIds:
    authorizeConnection_1489122155_1_Id = 1489122155
    batchGetItem_N697851100_1_Id = -697851100
    batchWriteItem_116217951_1_Id = 116217951
    defineAttributeList_670678385_1_Id = 670678385
    defineAttributeListId_N1230579644_1_Id = -1230579644
    defineKeySchema_N742646399_1_Id = -742646399
    deleteItem_1013539361_1_Id = 1013539361
    endpoints_455855874_1_Id = 455855874
    getItem_263244906_1_Id = 263244906
    methods_785068263_1_Id = 785068263
    putItem_N2106490455_1_Id = -2106490455
    query_N931250863_1_Id = -931250863
    scan_N1875390620_1_Id = -1875390620
    services_N1016793520_1_Id = -1016793520
    updateItem_1425579023_1_Id = 1425579023

class DaxResponseParam:
    Item = 0
    ConsumedCapacity = 1
    Attributes = 2
    ItemCollectionMetrics = 3
    Responses = 4
    UnprocessedKeys = 5
    UnprocessedItems = 6
    Items = 7
    Count = 8
    LastEvaluatedKey = 9
    ScannedCount = 10
    TableDescription = 11

class DaxDataRequestParam:
    ProjectionExpression = 0
    ExpressionAttributeNames = 1
    ConsistentRead = 2
    ReturnConsumedCapacity = 3
    ConditionExpression = 4
    ExpressionAttributeValues = 5
    ReturnItemCollectionMetrics = 6
    ReturnValues = 7
    UpdateExpression = 8
    ExclusiveStartKey = 9
    FilterExpression = 10
    IndexName = 11
    KeyConditionExpression = 12
    Limit = 13
    ScanIndexForward = 14
    Select = 15
    Segment = 16
    TotalSegments = 17
    RequestItems = 18

class ReturnConsumedCapacityValues:
    NONE = 0
    TOTAL = 1
    INDEXES = 2

class ReturnItemCollectionMetricsValue:
    NONE = 0
    SIZE = 1

class SelectValues:
    ALL_ATTRIBUTES = 1
    ALL_PROJECTED_ATTRIBUTES = 2
    COUNT = 3
    SPECIFIC_ATTRIBUTES = 4

class ReturnValues:
    NONE = 1
    ALL_OLD = 2
    UPDATED_OLD = 3
    ALL_NEW = 4
    UPDATED_NEW = 5

class ReturnValuesValues:
    NONE = 1
    ALL_OLD = 2
    UPDATED_OLD = 3
    ALL_NEW = 4
    UPDATED_NEW = 5

class ReturnItemCollectionMetricsValues:
    NONE = 0
    SIZE = 1



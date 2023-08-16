#!/bin/bash

# The URL to which you want to send the data
url="https://example.com/api/endpoint"

# JSON data to send in the request body
data='{"key1": "value1", "key2": "value2"}'

# Send the POST request with JSON data using curl
response=$(curl -s -X POST -H "Content-Type: application/json" -d "$data" "$url")

# Check the response
http_status=$(echo "$response" | awk '/HTTP/{print $2}')
if [ "$http_status" == "200" ]; then
    echo "Request was successful!"
    echo "Response: $response"
else
    echo "Request failed with status code: $http_status"
    echo "Response content: $response"
fi

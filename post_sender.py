import requests

# The URL to which you want to send the data
url = "https://example.com/api/endpoint"

# JSON data to send in the request body
data = {
    "key1": "value1",
    "key2": "value2"
}

# Send the POST request with JSON data
response = requests.post(url, json=data)

# Check the response
if response.status_code == 200:
    print("Request was successful!")
    print("Response:", response.json())
else:
    print("Request failed with status code:", response.status_code)
    print("Response content:", response.text)

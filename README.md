## HTTP pipelining

Effective with limited resources, for small and frequent requests.

Quite suitable for Airflow tasks.

Can also be used to get information about existing files in s3 storage:
```py
import boto3

conn = boto3.client('s3', 
        endpoint_url='http://localhost:9090',
        aws_access_key_id='...',
        aws_secret_access_key='...',
        config=boto3.session.Config(signature_version='s3v4'))

bucket = 'example-bt'
file_path = 'example.jpg'


from botocore.awsrequest import AWSRequest

request = AWSRequest()
request.method = 'HEAD'
request.url = f'http://localhost:9090/{bucket}/{file_path1}'

## small hack
conn._request_signer.sign('HeadObject', request)

print(request.headers)

### standard api request
## 2.7 sec

def heads_api():
    def wrap(**kw):
        try:
            conn.head_object(**kw)
            return True
        except Exception as ex:
            return False
    [wrap(Bucket=bucket, Key=f) for f in [file_path]* 1000]

### 'HEAD' requests 
def heads_requests():
  import requests
  ses = requests.Session()

  urls = [request.url] * 1000

  return [ses.head(rr, headers=request.headers) for rr in urls]

### true perfomance without multiplexing/threads...
## 0.7 sec
def heads_http_pipelining():
  from utils.http_pipelining import http_pipelining_with_retries
  return http_pipelining_with_retries(urls, method='HEAD', headers=request.headers)



```

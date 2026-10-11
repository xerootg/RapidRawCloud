# Reference SigV4 signatures via botocore (S3SigV4Auth) for the request shapes the firmware
# sends. Emits a flat text format consumed by host_tests/test_sigv4.c. The firmware's signer
# receives RAW (unencoded) path/query and must encode exactly like botocore/AWS.
import hashlib, base64, datetime
from urllib.parse import urlsplit, unquote
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

creds = Credentials("GKexampleaccesskey", "examplesecretkey0123456789abcdef")
FIXED = "20261010T123456Z"

class FixedAuth(S3SigV4Auth):
    def add_auth(self, request):
        request.context['timestamp'] = FIXED
        super().add_auth(request)

out = []
def add(name, method, url, headers, body=b"", region="garage"):
    req = AWSRequest(method=method, url=url, headers=dict(headers), data=body if body else None)
    FixedAuth(creds, "s3", region).add_auth(req)
    parts = urlsplit(url)
    out.append(f"CASE {name}")
    out.append(f"METHOD {method}")
    out.append(f"REGION {region}")
    out.append(f"HOST {parts.netloc}")
    out.append(f"PATH {unquote(parts.path)}")
    if parts.query:
        for pair in parts.query.split('&'):
            k, _, v = pair.partition('=')
            out.append(f"QUERY {unquote(k)}\t{unquote(v)}")
    for k, v in headers.items():
        out.append(f"HDR {k}\t{v}")
    out.append(f"DATE {req.headers['X-Amz-Date']}")
    out.append(f"PAYLOAD {req.headers['X-Amz-Content-SHA256']}")
    out.append(f"AUTH {req.headers['Authorization']}")
    out.append("END")

body = b"hello rrcloud\n"
md5b64 = base64.b64encode(hashlib.md5(body).digest()).decode()
add("put_simple_https", "PUT", "https://garage.example.com/rapidraw-cloud/library/Camera%20Import/NIKON%20Z%20f/2026/10/10/DSC_0001.NEF",
    {"content-md5": md5b64, "content-type": "application/octet-stream", "x-amz-meta-rrc-device": "0f6b2a1e-1111-4222-8333-944444444444"}, body)
add("put_simple_http_signed_payload", "PUT", "http://192.168.1.50:3900/bucket/library/a%2Bb%20c/x%20%26%20y.jpg",
    {"content-md5": md5b64, "content-type": "image/jpeg"}, body)
add("head", "HEAD", "http://192.168.1.50:3900/bucket/library/a%2Bb%20c/x.jpg", {}, b"")
add("get_range", "GET", "https://garage.example.com/bucket/.rrcloud/v1/manifests/x.json.gz", {"range": "bytes=0-1023"}, b"")
add("create_multipart", "POST", "https://garage.example.com/bucket/library/big.NEF?uploads", {"content-type": "application/octet-stream"}, b"")
add("upload_part", "PUT", "https://garage.example.com/bucket/library/big.NEF?partNumber=3&uploadId=abc%2Bdef%3D%3D", {"content-md5": md5b64}, body)
add("complete_multipart", "POST", "https://garage.example.com/bucket/library/big.NEF?uploadId=abc", {"content-type": "application/xml"}, b"<CompleteMultipartUpload></CompleteMultipartUpload>")
add("delete", "DELETE", "https://garage.example.com/bucket/.rrcloud/v1/journal/dev/0000000000000001.v1.ndjson", {}, b"")
add("list", "GET", "https://garage.example.com/bucket/?list-type=2&prefix=.rrcloud%2Fv1%2Fjournal%2F&max-keys=1000", {}, b"")
add("unicode_key", "PUT", "https://garage.example.com/bucket/library/caf%C3%A9/%E5%86%99%E7%9C%9F%20%281%29.JPG", {"content-type": "image/jpeg"}, body)
add("header_whitespace", "PUT", "https://s3.us-east-1.amazonaws.com/bucket/k", {"content-type": "  text/plain   ", "x-amz-meta-note": "a   b  c"}, body, region="us-east-1")
add("abort_multipart", "DELETE", "https://garage.example.com/bucket/library/big.NEF?uploadId=zz", {}, b"")
open("sigv4_vectors.txt", "w").write("\n".join(out) + "\n")
print(len(out), "lines")

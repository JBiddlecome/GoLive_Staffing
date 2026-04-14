import urllib.request
try:
    req = urllib.request.Request('http://localhost:8000/client-communication-summary')
    response = urllib.request.urlopen(req)
    print(response.getcode())
    print(response.read()[:1000])
except urllib.error.HTTPError as e:
    print(e.code)
    print(e.read().decode('utf-8'))
except Exception as e:
    print(e)

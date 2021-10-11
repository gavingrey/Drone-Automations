import json
import time
import secrets
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import jwt
import requests
from boxsdk import Client, OAuth2

def box_service():

    config = json.load(open('box_jwt_config.json'))

    appAuth = config["boxAppSettings"]["appAuth"]
    privateKey = appAuth["privateKey"]
    passphrase = appAuth["passphrase"]

    # https://cryptography.io/en/latest/
    key = load_pem_private_key(
      data=privateKey.encode('utf8'),
      password=passphrase.encode('utf8'),
      backend=default_backend(),
    )
    #print('key:', key)

    # Create JWT Assertion
    authentication_url = 'https://api.box.com/oauth2/token'

    claims = {
      'iss': config['boxAppSettings']['clientID'],
      'sub': config['enterpriseID'],
      'box_sub_type': 'enterprise',
      'aud': authentication_url,
      'jti': secrets.token_hex(64),
      'exp': round(time.time()) + 45
    }
    #print('claims:', claims)

    # Sign the claims by the private key
    keyId = config['boxAppSettings']['appAuth']['publicKeyID']

    assertion = jwt.encode(
      claims,
      key,
      algorithm='RS512',
      headers={
        'kid': keyId
      }
    )
    #print('assertion:', assertion)

    # Request access token
    params = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': assertion,
        'client_id': config['boxAppSettings']['clientID'],
        'client_secret': config['boxAppSettings']['clientSecret']
    }
    response = requests.post(authentication_url, params)
    access_token = response.json()['access_token']
    #print('access_token:', access_token)

    auth = OAuth2(
    client_id='cnaz7hvo3uzqlhfuhirvk3ayvsig5x4j', 
    client_secret='', 
    access_token=access_token
    )
    box_client = Client(auth)
    #print('box_client:', box_client)
    return box_client

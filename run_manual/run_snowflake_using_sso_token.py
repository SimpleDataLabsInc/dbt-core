import base64
import logging
from logging import getLogger
from time import sleep

import requests
import snowflake.connector
import snowflake.connector.errors

logger = getLogger(__name__)

# Taken from snowflake
# with
#
# integration_secrets as (
#   select parse_json(system$show_oauth_client_secrets('DBT_CLOUD')) as secrets
# )
#
# select
#   secrets:"OAUTH_CLIENT_ID"::string     as client_id,
#   secrets:"OAUTH_CLIENT_SECRET"::string as client_secret
# from
#   integration_secrets;
oauth_client_id = "p/8gIkjOiCvOmAeJlAUVw9/MC2g="
oauth_client_secret = "XKxbCDVaT2pE4f2eDNniGi99+d+XSutJHtP7kM5BziU="
# This will be refresh token
token = "ver:2-hint:20005396485-did:1023-ETMsDgAAAY274CpLABRBRVMvQ0JDL1BLQ1M1UGFkZGluZwEAABAAEDPGRwtwwyNWqWotZt/tBtEAAAEAwH8TP/SmS7bgg5BMxgkZDEDomMQEVn3YdYWVD5oGJgFce36OJLZeaDxVHKZPkjXFzp6L2Ey6E/gTXIMq4ewpP/GZ52Qp8sMsnPLkRzoja6A4Gd6BFLhgCkEU7GDbsu9YDLAhvzlANGH8tKoCzGl9liNRYB7Yinb5FO4ybYgz6s7+cregPOe6k+A33UU32myAhGQ8s71vAg23scdOfqNcTlbQoW9Jk5V9D00I9wdZTAHi78BtPQSRsLc4fw7A7AC4BInKeuRM4U+wBKxegKuXB450KW5mRKIJSvSYrsvpn3p+IIReS+pqO/4xyNB/GWW53vB6fut6MHSJMBiA6h0kMAAUudm/0WD+WSKCryMDrOkgRMH7UpU="
authenticator = "oauth"
account = "rmfxjmu-rg03662"
user = "testsso"
password = ""
database = "QA_DATABASE"
schema = "qa_simple_schema"
warehouse = "COMPUTE_WH"
role = "DEV_ROLE"
query_tag = "snowflake"
host = ""
port = ""
proxy_host = ""
proxy_port = ""
protocol = ""


def _get_access_token() -> str:
    if authenticator != "oauth":
        raise "Can only get access tokens for oauth"
    missing = any(
        x is None for x in (oauth_client_id, oauth_client_secret, token)
    )
    if missing:
        raise "need a client ID a client secret, and a refresh token to get " "an access token"

    _TOKEN_REQUEST_URL = "https://{}.snowflakecomputing.com/oauth/token-request"

    # should the full url be a config item?
    token_url = _TOKEN_REQUEST_URL.format(account)
    # I think this is only used to redirect on success, which we ignore
    # (it does not have to match the integration's settings in snowflake)
    redirect_uri = "http://localhost:9999"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": token,
        "redirect_uri": redirect_uri,
    }

    auth = base64.b64encode(
        f"{oauth_client_id}:{oauth_client_secret}".encode("ascii")
    ).decode("ascii")
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-type": "application/x-www-form-urlencoded;charset=utf-8",
    }

    result_json = None
    max_iter = 20
    # Attempt to obtain JSON for 1 second before throwing an error
    for i in range(max_iter):
        result = requests.post(token_url, headers=headers, data=data)
        try:
            result_json = result.json()
            break
        except ValueError as e:
            message = result.text
            logger.debug(
                f"Got a non-json response ({result.status_code}): \
                              {e}, message: {message}"
            )
            sleep(0.05)

    if result_json is None:
        raise f"""Did not receive valid json with access_token.
                                        Showing json response: {result_json}"""

    print(result_json)
    return result_json["access_token"]


def get_auth_args():
    result = {}
    if password:
        result["password"] = password
    if host:
        result["host"] = host
    if port:
        result["port"] = port
    if proxy_host:
        result["proxy_host"] = proxy_host
    if proxy_port:
        result["proxy_port"] = proxy_port
    if protocol:
        result["protocol"] = protocol
    if authenticator:
        result["authenticator"] = authenticator
        if authenticator == "oauth":
            localToken = token
            # if we have a client ID/client secret, the token is a refresh
            # token, not an access token
            if oauth_client_id and oauth_client_secret:
                localToken = _get_access_token()
            elif oauth_client_id:
                raise "Invalid profile: got an oauth_client_id, but not an oauth_client_secret!"
            elif oauth_client_secret:
                raise "Invalid profile: got an oauth_client_secret, but not an oauth_client_id!"

            result["token"] = localToken
        # enable id token cache for linux
        result["client_store_temporary_credential"] = True
        # enable mfa token cache for linux
        result["client_request_mfa_token"] = True
    result["reuse_connections"] = True
    result["private_key"] = None
    return result


logging.basicConfig(level=logging.DEBUG)  # Set logging level to DEBUG

if __name__ == '__main__':
    auth_args = get_auth_args()
    session_parameters = {}
    session_parameters.update({"QUERY_TAG": query_tag})
    handle = snowflake.connector.connect(
        account=account,
        user=user,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role,
        autocommit=True,
        client_session_keep_alive=False,
        application="dbt",
        insecure_mode=False,
        session_parameters=session_parameters,
        **auth_args,
    )
    # Create cursor
    cursor = handle.cursor()

    # Execute query
    query = '''SELECT 1,2,3, 'random' '''
    cursor.execute(query)

    # Fetch and print results
    for row in cursor.fetchall():
        print(row)

    # Close cursor and connection
    cursor.close()
    handle.close()

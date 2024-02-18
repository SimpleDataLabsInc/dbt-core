import base64
import logging
from logging import getLogger
from time import sleep

import requests
import snowflake.connector
import snowflake.connector.errors

logger = getLogger(__name__)

oauth_client_id = None
oauth_client_secret = None
token = None
authenticator = "https://dev-1571692.okta.com/home/snowflake/0oad5uhrilw6JXySi5d7/54463"
account = "rmfxjmu-rg03662"
user = "vishal@simpledatalabs.com"
password = "Prophecy@123"
database = "QA_DATABASE"
schema = "qa_simple_schema"
warehouse = "COMPUTE_WH"
role = "ACCOUNTADMIN"
query_tag = "snowflake"
token = ""
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
            # token = token
            # if we have a client ID/client secret, the token is a refresh
            # token, not an access token
            if oauth_client_id and oauth_client_secret:
                token = _get_access_token()
            elif oauth_client_id:
                raise "Invalid profile: got an oauth_client_id, but not an oauth_client_secret!"
            elif oauth_client_secret:
                raise "Invalid profile: got an oauth_client_secret, but not an oauth_client_id!"

            result["token"] = token
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
    query = 'SELECT 1,2,3'
    cursor.execute(query)

    # Fetch and print results
    for row in cursor.fetchall():
        print(row)

    # Close cursor and connection
    cursor.close()
    handle.close()

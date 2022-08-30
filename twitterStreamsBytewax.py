import json
import os
import requests
from bytewax import Dataflow, run

#add your token generated on twitter developer account
token = "TWITTER_BEARER_TOKEN"

def add_bearerOAuth(req):
    req.headers["Authorization"] = f"Bearer {token}"
    req.headers["User-Agent"] = "v2FilteredStreamPython"
    return req

def setRules(search_rules):
    payload = {"add": search_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=add_bearerOAuth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception("Cannot add rules (HTTP {}): {}".format(response.status_code, response.text))
    print(json.dumps(response.json()))

def getStream(flow):
    result = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=add_bearerOAuth, stream=True,
    )
    print(result.status_code)
    if result.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                result.status_code, result.text
            )
        )
    for resultLine in result.iter_lines():
        if resultLine:
            jsonResult = json.loads(resultLine)
            jsonResult = json.dumps(jsonResult, indent=4, sort_keys=True)
            print("-" * 200)
            jsonResult = json.loads(jsonResult)
            tweet = jsonResult["data"]["text"]
            flow_input = [(0, tweet)]
            print(run(flow, flow_input))
            print("**--------------------****--------------------****--------------------**")

def getAllRules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=add_bearerOAuth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def deleteAllRules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=add_bearerOAuth,
        json=payload,
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def hasPrice(tweet):
    """
    Gets tweets that has the word price
    :param tweet:
    :return: tweet that has price
    """
    tweet=tweet.lower()
    words=tweet.split()
    if "price" in words :
        with open("tweets.txt", "a") as f:
            f.write(str(tweet)+"\n")
        return tweet

def main():
    allRules = getAllRules()
    deleteAllRules(allRules)
    flow = Dataflow()
    flow.map(hasPrice)
    flow.capture()
    setRules([{"value": "#BTC lang:en -is:reply -is:retweet", "tag": "BTC"}])
    getStream(flow)

if __name__ == "__main__":
    main()

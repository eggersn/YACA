import json
import uuid
import os
import base64
import random
from nacl.signing import SigningKey

def setup():
    f = open("config/server.json")
    config = json.load(f)

    data = {} 

    for i in range(config["initial"]["instances"]):
        identifier = config["initial"]["specs"][i]["id"]
        data[identifier] = {}
        data[identifier]["ip_addr"] = config["initial"]["specs"][i]["ip_addr"]
        data[identifier]["port"] = config["initial"]["specs"][i]["port"]

    data["global"] = {}
    data["global"]["pks"] = {}
    data["global"]["ports"] = {}
    data["global"]["ip_addrs"] = {}

    for identifier in data:
        if identifier != "global":
            data[identifier]["id"] = identifier
            data[identifier]["servers"] = list(data.keys())
            data[identifier]["servers"].remove("global")

            key = SigningKey.generate()
            data[identifier]["sk"] = base64.b64encode(key.encode()).decode("ascii")

            for identifier1 in data:
                if "pks" not in data[identifier1]:
                    data[identifier1]["pks"] = {}
                    data[identifier1]["ports"] = {}
                    data[identifier1]["ip_addrs"] = {}
                data[identifier1]["pks"][identifier] = base64.b64encode(key.verify_key.encode()).decode("ascii")
                data[identifier1]["ip_addrs"][identifier] = data[identifier]["ip_addr"]
                data[identifier1]["ports"][identifier] = data[identifier]["port"]


            data["global"]["pks"][identifier] = base64.b64encode(key.verify_key.encode()).decode("ascii")
            data["global"]["ip_addrs"][identifier] = data[identifier]["ip_addr"]
            data["global"]["ports"][identifier] = data[identifier]["port"]


    if os.path.isdir("config/" + config["initial"]["path"]):
        for f in os.listdir("config/" + config["initial"]["path"] + "/"):
            os.remove(os.path.join("config/" + config["initial"]["path"] + "/", f))
    else:
        os.mkdir("config/" + config["initial"]["path"])

    for identifier in data:
        with open("config/" + config["initial"]["path"] + "/" + identifier + ".json", 'w') as outfile:
            json.dump(data[identifier], outfile)

if __name__ == "__main__":
    setup()
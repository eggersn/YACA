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

    for _ in range(config["initial"]["instances"]):
        identifier = str(uuid.uuid4())
        data[identifier] = {}

    for identifier in data:
        data[identifier]["id"] = identifier
        data[identifier]["servers"] = list(data.keys())

        key = SigningKey.generate()
        data[identifier]["sk"] = base64.b64encode(key.encode()).decode("ascii")
        data[identifier]["port"] = random.randint(10000, 40000)

        for identifier1 in data:
            if "pks" not in data[identifier1]:
                data[identifier1]["pks"] = {}
                data[identifier1]["ports"] = {}
            data[identifier1]["pks"][identifier] = base64.b64encode(key.verify_key.encode()).decode("ascii")
            data[identifier1]["ports"][identifier] = data[identifier]["port"]

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
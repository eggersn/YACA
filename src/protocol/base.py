import json
import base64
from src.core.group_view.group_view import GroupView
from src.core.signatures.signatures import Signatures


class Message:
    def __init__(self):
        self.header: str
        self.content: dict
        self.meta: dict = {}
        self.json_data: str
        

    def encode(self):
        self.json_data = json.dumps(
            {"header": self.header, "content": self.content, "meta": self.meta}
        )

    def sign(self, sk: Signatures):
        if not self.is_decoded:
            self.decode()

        reduced_json_data = json.dumps(
            {
                "header": self.header,
                "content": self.content,
                "meta": {k: self.meta[k] for k in self.meta if k != "signature"},
            }
        )
        signature = sk.sign(reduced_json_data.encode())
        self.meta["signature"] = (
            sk.identifier,
            base64.b64encode(signature.signature).decode("ascii"),
        )

        self.encode()

    def decode(self):
        data = json.loads(self.json_data)
        self.header = data["header"]
        self.content = data["content"]
        self.meta = data["meta"]

    def verify_signature(self, sk: Signatures, group_view: GroupView):
        if not self.is_decoded:
            self.decode()

        identifier = self.meta["signature"][0]
        
        if group_view is not None and not group_view.check_if_participant(identifier):
            if "HeartBeat" not in self.header:
                print("Signature: Not participant of group", identifier)
            return False

        signature = base64.b64decode(self.meta["signature"][1])
        data = json.dumps(
            {
                "header": self.header,
                "content": self.content,
                "meta": {k: self.meta[k] for k in self.meta if k != "signature"},
            }
        ).encode()

        valid = sk.check_validity(data, signature, group_view.pks[identifier])

        return valid

    def get_signature(self):
        if self.is_signed:
            return self.meta["signature"]
        else:
            return None, None

    def set_sender(self, addr):
        if "sender" not in self.meta:
            self.meta["sender"] = addr

    def get_sender(self):
        if "sender" in self.meta:
            return tuple(self.meta["sender"])

    def get_topic(self):
        if self.has_topic:
            return self.meta["topic"]
        else:
            return ""

    def set_nonce(self, nonce : str):
        self.meta["nonce"] = nonce 
    
    def get_nonce(self):
        if self.has_nonce:
            return self.meta["nonce"]
        else:
            return ""

    @property
    def is_encoded(self):
        if hasattr(self, "json_data"):
            return True
        return False

    @property
    def is_decoded(self):
        if hasattr(self, "header"):
            return True
        return False

    @property 
    def is_signed(self):
        if not self.is_decoded:
            self.decode()

        return "signature" in self.meta

    @property
    def has_topic(self):
        if not self.is_decoded:
            self.decode()

        return "topic" in self.meta

    @property 
    def has_nonce(self):
        if not self.is_decoded:
            self.decode()

        return "nonce" in self.meta

    @classmethod
    def initFromJSON(cls, json_data):
        message = cls()
        message.json_data = json_data
        return message

    @classmethod
    def initFromData(cls, header, content={}, meta={}):
        message = cls()
        message.header = header
        message.content = content
        message.meta = meta
        return message

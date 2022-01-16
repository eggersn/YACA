import json
import base64
from src.core.group_view.group_view import GroupView
from src.core.signatures.signatures import Signatures


class Message:
    header: str
    content: dict
    meta: dict = {}
    json_data: str

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
        
        if not group_view.check_if_participant(identifier):
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

    @property
    def is_encoded(self):
        if hasattr(self, "json_data"):
            return True
        return False

    @property
    def is_decoded(self):
        if hasattr(self, "content"):
            return True
        return False

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

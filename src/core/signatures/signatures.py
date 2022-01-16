from nacl.signing import SigningKey, VerifyKey
from nacl.exceptions import BadSignatureError


class Signatures:
    def __init__(self, sk: SigningKey, identifier : str):
        self.identifier = identifier
        if sk != 0:
            self.sk = sk
        else:
            self.sk = SigningKey.generate()

    def sign(self, data: bytes):
        return self.sk.sign(data)

    def check_validity(self, data: bytes, signature: bytes, verify_key: VerifyKey) -> bool:
        try:
            verify_key.verify(data, signature)
        except BadSignatureError as err:
            print(err)
            return False

        return True

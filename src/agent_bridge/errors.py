class BridgeError(Exception):
    def __init__(self, code: str, message: str, provider_limit=None):
        super().__init__(message)
        self.code = code
        self.provider_limit = provider_limit

    def as_dict(self):
        return {"code": self.code, "message": str(self)}

class FailedInitialization(Exception):
    def __init__(self, service_name = None) -> None:
        message = self._construct_message(service_name)
        super().__init__(message)

    def _construct_message(self, service_name) -> str:
        return f"Module {service_name} couldn't be initialized"

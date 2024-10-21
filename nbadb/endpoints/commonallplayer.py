from nbadb.endpoints.base import EndpointJobFactory


class CommonAllPlayersFactory(EndpointJobFactory):
    def __init__(self):
        super().__init__("commonallplayers")

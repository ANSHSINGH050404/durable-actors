class Effects:
    def __init__(self):
        self.published = []
        self.connections = []
        self.admissions = 0

    async def publish(self, effects):
        self.published.extend(effects)

    async def get_connections(self, tag=None, count_only=False):
        if count_only:
            return len(self.connections)
        return [
            connection
            for connection in self.connections
            if tag is None or tag in connection["tags"]
        ]

    def admit(self):
        self.admissions += 1

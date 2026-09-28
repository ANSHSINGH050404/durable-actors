class Effects:
    def __init__(self):
        self.published = []
        self.connections = []
        self.admissions = 0

    async def publish(self, effects):
        self.published.extend(effects)

    async def get_connections(self):
        return self.connections

    def admit(self):
        self.admissions += 1

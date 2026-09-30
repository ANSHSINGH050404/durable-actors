from durable_actors import Actor, persisted


class Counter(Actor):
    count: int = persisted(0)

    def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count

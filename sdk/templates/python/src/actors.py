from durable_actors import Actor


class Counter(Actor):
    count: int = 0

    def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count

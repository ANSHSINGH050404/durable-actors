from durable_actors import Actor, ActorSocket, emitted
from pydantic import BaseModel, Field


class Member(BaseModel):
    """Identity attached to a chat connection."""

    name: str


class Message(BaseModel):
    """A message saved in the chat history."""

    text: str = Field(description="The message body.")


class Chat(Actor[Member, Message, Message]):
    """A durable chat room with live message history."""

    messages: list[Message] = emitted(default_factory=list)

    def append(self, message: Message) -> list[Message]:
        """Save and broadcast a message, returning the complete chat history."""
        self.messages.append(message)
        self.broadcast(message)
        return self.messages

    def on_message(
        self, socket: ActorSocket[Member, Message], message: Message
    ) -> None:
        self.append(Message(text=f"{socket.metadata.name}: {message.text}"))

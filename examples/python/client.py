from generated import actors

messages = actors.Chat.get("lobby").append(actors.Chat.Message(text="Hello from Python"))
print(messages)

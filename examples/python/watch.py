from generated import actors

chat = actors.Chat.get("lobby")
subscription = chat.subscribe(
    lambda state: print(state.messages), metadata=actors.Chat.Metadata(name="Ada")
)
try:
    chat.append(actors.Chat.Message(text="Hello from a state subscriber"))
    input("Listening for state changes. Press Enter to stop.\n")
finally:
    subscription.close()

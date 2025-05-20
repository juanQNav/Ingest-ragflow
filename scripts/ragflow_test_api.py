from ragflow_sdk import RAGFlow
rag_object = RAGFlow(
    api_key="ragflow-I5ZDJlNjI4ZTMwYzExZWZhYzA1MDI0Mm",
    base_url="http://localhost:9380"
)

name_assistants = []

for i, assistant in enumerate(rag_object.list_chats()):
    print(f'id ({i}): {assistant.name} ')
    name_assistants.append(assistant.name)

id_n = int(input("Enter the id of the assistant you want to test: "))

name_as = name_assistants[id_n]

print(name_as)
assistant = rag_object.list_chats(name=name_as)
print(assistant)
assistant = assistant[0]
session = assistant.create_session()

print("\n==================== AVRI =====================\n")
print("Hello. What can I do for you?")

while True:
    question = input("\n==================== JQNAV =====================\n> ")
    print("\n==================== AVRI =====================\n")

    cont = ""
    for ans in session.ask(question, stream=True):
        print(ans.content[len(cont):], end='', flush=True)
        cont = ans.content

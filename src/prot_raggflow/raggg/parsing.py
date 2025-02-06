import asyncio

async def monitor_parsing(dataset, document_ids, pbar):
    all_done = False
    while not all_done:
        all_done = True
        done_count = 0

        documents = dataset.list_documents()
        for doc in documents:
            if doc.id in document_ids:
                if doc.run == "RUNNING":
                    all_done = False
                else:
                    done_count += 1

        pbar.set_postfix({"Completed": done_count})
        pbar.n = done_count
        pbar.last_print_n = done_count
        pbar.refresh()

        if not all_done:
            await asyncio.sleep(1)

    print("Parsing completed for all documents.")

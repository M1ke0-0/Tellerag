from source.DynamicConfigurationLoading import get_config
import chromadb

settings = get_config()

chroma_client = chromadb.HttpClient(
    host=settings.RAG_HOST,
    port=settings.RAG_PORT,
    ssl=False,
    headers=None
)

collection = chroma_client.get_or_create_collection(name="my_collection")

# collection.add(
#     documents=[
#         "BREAKING NEWS: Dog rescued from tree",
#         # "This is a document about oranges"
#     ],
#     ids=["id3"]
# )

results = collection.query(
    # Chroma will embed this for you
    query_texts=["123"],
    n_results=1  # how many results to return
)
print(results)

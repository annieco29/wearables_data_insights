import json
import os
import chromadb

# for each json file in processed_data directory read in the file and create a vector embedding
# save the vector embedding in a vectorstore with the file name as the key

# Initialize ChromaDB client
chroma_client = chromadb.PersistentClient(path="./chroma_db") 
collection = chroma_client.get_or_create_collection(name="medical_articles")

# Directory containing JSON files
PROCESSED_DATA_DIR = "./processed_data"

# Loop through all JSON files in the directory
for filename in os.listdir(PROCESSED_DATA_DIR):
    if filename.endswith(".json"):  # Process only JSON files
        file_path = os.path.join(PROCESSED_DATA_DIR, filename)

        # Read JSON file
        with open(file_path, "r", encoding="utf-8") as json_file:
            article = json.load(json_file)

        # Extract article title and text
        article_id = article.get("id", filename)  # Use filename if no ID
        title = article.get("title", "Untitled Article")
        text = article.get("text", "")

        # Add document to ChromaDB with automatic embeddings
        collection.add(
            ids=[article_id],
            documents=[text],
            metadatas=[{"title": title, "filename": filename}]
        )

        print(f"Added: {title} ({filename})")

print("All articles have been embedded and stored in ChromaDB!")


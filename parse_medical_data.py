import os
import json
import tarfile
import xmltodict
from google.cloud import storage
from unstructured.partition.xml import partition_xml

# set GCS bucket and local processing directory
BUCKET_NAME = "wearables-data-insights"
LOCAL_DOWNLOAD_DIR = "./downloads"
EXTRACTION_DIR = "./extracted" 
PROCESSED_DATA_DIR = "./processed_data"

# ensure directories exist
os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)
os.makedirs(EXTRACTION_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# Initialize GCS client
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

# List .tar.gz files in GCS bucket
blobs = list(bucket.list_blobs(prefix=""))

# Download and extract only the FIRST .tar.gz file
for blob in blobs:
    if blob.name.endswith(".tar.gz"):
        local_file_path = os.path.join(LOCAL_DOWNLOAD_DIR, os.path.basename(blob.name))

        blob.download_to_filename(local_file_path)
        print(f"Download {blob.name} complete: {local_file_path}")

        # Extract the .tar.gz file
        print(f"Extracting {local_file_path} to {EXTRACTION_DIR}...")
        with tarfile.open(local_file_path, "r:gz") as tar:
            tar.extractall(EXTRACTION_DIR)
        print(f"Extraction complete: {EXTRACTION_DIR}")

        break  # Stop after the first .tar.gz file

# Function to process XML
def process_xml(xml_file):
    """
    Reads and preprocesses an XML file using the `unstructured` library.
    """
    with open(xml_file, "r", encoding="utf-8") as f:
        xml_content = f.read()

    # Convert XML to structured text
    elements = partition_xml(text=xml_content)

    # Extract clean text from elements
    processed_text = "\n".join([element.text for element in elements if element.text])

    return processed_text

# Find and process the FIRST XML file
# xml_found = False
for root_dir, _, files in os.walk(EXTRACTION_DIR):
    for file in files:
        if file.endswith(".xml"):
            xml_path = os.path.join(root_dir, file)
            print(f"Processing XML file: {xml_path}...")

            # Process XML and extract clean text
            clean_text = process_xml(xml_path)

            # Save processed text as JSON
            output_json_path = os.path.join(PROCESSED_DATA_DIR, f"{file}.json")
            with open(output_json_path, "w", encoding="utf-8") as json_file:
                json.dump({"filename": file, "text": clean_text}, json_file, indent=4)

            print(f"Saved processed text to {output_json_path}")

            # xml_found = True
           # break  # Stop after processing the first XML file
    # if xml_found:
        #break  # Exit after processing the first XML

print("Processing complete. Ready for RAG indexing!")


# DLAI_API_KEY = utils.get_dlai_api_key()
# DLAI_API_URL = utils.get_dlai_url()

# s = UnstructuredClient(
#     api_key_auth=DLAI_API_KEY,
#     server_url=DLAI_API_URL,
# )

# filename = "data.nosync/"
# elements = partition_xml(filename=filename)
# element_dict = [el.to_dict() for el in elements]
# example_output = json.dumps(element_dict[11:15], indent=2)
# print(example_output)
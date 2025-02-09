import os
import json
import tarfile
import xmltodict
import concurrent.futures
from tqdm import tqdm
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

# Download and extract all .tar.gz files
for blob in tqdm(blobs, desc="Downloading .tar.gz files"):
    if blob.name.endswith(".tar.gz"):
        local_file_path = os.path.join(LOCAL_DOWNLOAD_DIR, os.path.basename(blob.name))

        # Skip download if file already exists
        if not os.path.exists(local_file_path):
            blob.download_to_filename(local_file_path)
            print(f"Download complete: {local_file_path}")

        # Extract .tar.gz file
        extraction_subdir = os.path.join(EXTRACTION_DIR, os.path.splitext(os.path.basename(blob.name))[0])
        os.makedirs(extraction_subdir, exist_ok=True)

        print(f"Extracting {local_file_path} to {extraction_subdir}...")
        with tarfile.open(local_file_path, "r:gz") as tar:
            tar.extractall(extraction_subdir)
        print(f"Extraction complete: {extraction_subdir}")

# Function to process an XML file
def process_xml_file(xml_path):
    """Reads and preprocesses an XML file using `unstructured`."""
    try:
        with open(xml_path, "r", encoding="utf-8") as f:
            xml_content = f.read()

        # Convert XML to structured text
        elements = partition_xml(text=xml_content)

        # Extract clean text from elements
        processed_text = "\n".join([element.text for element in elements if element.text])

        # Save processed text as JSON
        json_filename = os.path.basename(xml_path) + ".json"
        output_json_path = os.path.join(PROCESSED_DATA_DIR, json_filename)

        with open(output_json_path, "w", encoding="utf-8") as json_file:
            json.dump({"filename": os.path.basename(xml_path), "text": processed_text}, json_file, indent=4)

        return f"Processed: {xml_path}"
    
    except Exception as e:
        return f"Error processing {xml_path}: {e}"

# Find all XML files
xml_files = []
for root, _, files in os.walk(EXTRACTION_DIR):
    for file in files:
        if file.endswith(".xml"):
            xml_files.append(os.path.join(root, file))

print(f"Found {len(xml_files)} XML files to process.")

# Process XML files in parallel (multiprocessing)
with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
    results = list(tqdm(executor.map(process_xml_file, xml_files), total=len(xml_files), desc="Processing XML files"))

# Print completion message
print("\n".join(results))
print("All XML files have been processed and stored in JSON format!")

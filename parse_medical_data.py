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

# # Function to Extract `.tar.gz` Properly
# def extract_tar_gz(tar_gz_path, extraction_dir):
#     """Extracts .tar.gz files and ensures XML files are properly extracted."""
#     with tarfile.open(tar_gz_path, "r:gz") as tar:
#         tar.extractall(extraction_dir)

#     # Check if a .tar file was extracted (nested archive case)
#     for item in os.listdir(extraction_dir):
#         if item.endswith(".tar"):
#             nested_tar_path = os.path.join(extraction_dir, item)
#             with tarfile.open(nested_tar_path, "r:") as nested_tar:
#                 nested_tar.extractall(extraction_dir)  # Extract final XMLs
#             os.remove(nested_tar_path)  # Cleanup the .tar file

# # Download and extract all .tar.gz files
# for blob in tqdm(blobs, desc="Downloading .tar.gz files"):
#     if blob.name.endswith(".tar.gz"):
#         local_file_path = os.path.join(LOCAL_DOWNLOAD_DIR, os.path.basename(blob.name))

#         # Skip download if file already exists
#         if not os.path.exists(local_file_path):
#             blob.download_to_filename(local_file_path)
#             print(f"Download complete: {local_file_path}")

#         # Extract the `.tar.gz` and `.tar` file
#         extraction_subdir = os.path.join(EXTRACTION_DIR, os.path.splitext(os.path.basename(blob.name))[0])
#         os.makedirs(extraction_subdir, exist_ok=True)

#         print(f"Extracting {local_file_path} to {extraction_subdir}...")
#         extract_tar_gz(local_file_path, extraction_subdir)
#         print(f"Extraction complete: {extraction_subdir}")

def process_xml_file(xml_path):
    """Reads and preprocesses an XML file using `unstructured`."""
    try:
        if os.path.getsize(xml_path) > 10 * 1024 * 1024:
            return f"⚠ Skipping large file: {xml_path}"

        with open(xml_path, "r", encoding="utf-8") as f:
            xml_content = f.read()

        elements = partition_xml(text=xml_content)
        processed_text = "\n".join([element.text for element in elements if element.text])

        json_filename = os.path.basename(xml_path) + ".json"
        output_json_path = os.path.join(PROCESSED_DATA_DIR, json_filename)

        with open(output_json_path, "w", encoding="utf-8") as json_file:
            json.dump({"filename": os.path.basename(xml_path), "text": processed_text}, json_file, indent=4)

        return f"Processed: {xml_path}"
    
    except Exception as e:
        return f"Error processing {xml_path}: {e}"

BATCH_SIZE = 10000  # Adjust based on available RAM

if __name__ == "__main__":
    xml_files = [os.path.join(root, file) for root, _, files in os.walk(EXTRACTION_DIR) for file in files if file.endswith(".xml")]

    print(f"Found {len(xml_files)} XML files to process.")

    for i in range(0, len(xml_files), BATCH_SIZE):
        batch = xml_files[i:i + BATCH_SIZE]
        print(f"Processing batch {i // BATCH_SIZE + 1} of {len(xml_files) // BATCH_SIZE + 1}...")

        with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
            results = list(tqdm(executor.map(process_xml_file, batch), total=len(batch), desc="Processing XML files"))

        with open("processing_log.txt", "a") as log_file:
            log_file.write("\n".join(results) + "\n")
        print(f"✅ Processed {len(batch)} files - Log saved to processing_log.txt")


    print("All XML files have been processed in batches!")




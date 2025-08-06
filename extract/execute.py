import os
import sys
import requests
import time
from pyspark.sql import SparkSession
from zipfile import ZipFile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time

def create_spark_session(logger):
    """Initialize Spark session."""
    logger.debug("Initializing Spark Session with default parameters")
    return SparkSession.builder \
        .appName("ETL Extract") \
        .getOrCreate()

def download_zip_file(logger, url, output_dir):
    """Download zip file from given URL."""
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded zip file: {filename}")
        return filename
    else:
        error_msg = f"Failed to download file: Status code {response.status_code}"
        logger.error(error_msg)
        raise Exception(error_msg)

def extract_zip_file(logger, zip_filename, output_dir):
    """Extract contents of zip file."""
    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)

    logger.info(f"Extracted files written to: {output_dir}")
    logger.debug("Removing the zip file")
    os.remove(zip_filename)

def fix_json_dict(logger, output_dir):
    """Fix the format of artists JSON file."""
    import json
    file_path = os.path.join(output_dir, "dict_artists.json")
    
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        fixed_path = os.path.join(output_dir, "fixed_da.json")
        with open(fixed_path, "w", encoding="utf-8") as f_out:
            for key, value in data.items():
                record = {"id": key, "related_ids": value}
                json.dump(record, f_out, ensure_ascii=False)
                f_out.write("\n")
        
        logger.info(f"File {file_path} has been fixed and written to {fixed_path}")
        logger.debug("Removing the original file")
        os.remove(file_path)
    except Exception as e:
        logger.error(f"Error processing JSON file: {e}")
        raise

if __name__ == "__main__":
    logger = setup_logging("extract.log")
    
    if len(sys.argv) < 2:
        logger.error("Extraction path is required")
        logger.error("Example Usage:")
        logger.error("python3 execute.py /home/ishpa-maharjan/Data/Extraction")
        sys.exit(1)
    
    try:
        logger.info("Starting Extraction Engine...")
        start_time = time.time()
        
        EXTRACT_PATH = sys.argv[1]
        KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250725%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250725T024433Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=66b81df93ffcbc0e45a256627aa3103fd730d80b075466bf44858b663448bbde8f7d3a180a83c4c58851968eba86ff7aeb9ea4b495f1e59295c1f4ed2e4214825d0c356a2e4154e03ebda88b0bbcc88ce61287d13a647bef99ee7dc987622db8b6a3d03199944eb7bb4b788e05d68bb021dc1d832d4c01d1719358db1034a524291cf12fbebaf567d9291e6e9c7ef633233d55a6f64d6af36a315b92952d9a0ff5b805e1c24b1ef8189ea937e17a172185ef74995f7b351d41081893b98ca8da5707a5567ad35243e46577cb9c3edbe887125825aec3aef576e20989779397c7de16da9db1169458bb3855670418e78ec204f9ee0ae8f53632a645ba0edd0432"
        
        zip_filename = download_zip_file(logger, KAGGLE_URL, EXTRACT_PATH)
        extract_zip_file(logger, zip_filename, EXTRACT_PATH)
        fix_json_dict(logger, EXTRACT_PATH)
        
        end_time = time.time()
        logger.info("Extraction Successfully Completed!!!")
        logger.info(f"Total time taken: {format_time(end_time - start_time)}")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)
import requests
from bs4 import BeautifulSoup
import re
import json
import os
import pandas as pd
import glob

SOURCES_BASE = "https://ico.org.uk/action-weve-taken/complaints-and-concerns-data-sets"
BASE_URL = "https://ico.org.uk/"

def fetch_ico_data(source_root="self-reported-personal-data-breach-cases"):
    """
    Load the data from the ICO website.
    """
    source_page = f"{SOURCES_BASE}/{source_root}"
    response = requests.get(source_page)
    if response.status_code != 200:
        raise Exception(f"Failed to load data from {source_page}, status code: {response.status_code}")
    soup = BeautifulSoup(response.text, "html.parser")

    ## x-* attributes are pseudo-attributes, with custom meaning
    ## x-href attributes may store links but give custom behaviours
    links = soup.find_all("further-reading", {"x-href": True})  # Get all links with x-href attribute
    links = [link["x-href"] for link in links]
    links = [BASE_URL + link for link in links]  # Resolve relative URLs

    unknown_counter = 0
    source_directory = source_root.replace("-", "_")

    # Create directory if it doesn't exist
    if not os.path.exists(f"data/{source_directory}"):
        os.makedirs(f"data/{source_directory}")

    for link in links:
        response = requests.get(link)
        if response.status_code != 200:
            print(f"Failed to download {link}: Status code {response.status_code}")
            continue

        # We are going to parse the time period from the filename
        filename = link.split("/")[-1]  # Extract filename from URL

        fy, fy_quarter = extract_fy_quarter_from_filename(filename)
        file_save_path = f"data/{source_directory}/FY_{fy}_Q{fy_quarter}"
        if fy_quarter == "Unknown" or fy == "Unknown":
            unknown_counter += 1
            file_save_path = f"{file_save_path}_{unknown_counter}"
        with open(file_save_path + ".csv", "wb") as file:
            file.write(response.content)
        print(f"Downloaded: {filename}")

def extract_fy_quarter_from_filename(filename):
    """
    Extract fiscal year and quarter from the filename.
    """
    # file names include patterns q[1-4] and either 20\d\d-20\d\d or 20\d\d\d\d
    fy_quarter = re.search(r'q([1-4])', filename)
    fy = re.search(r'(20\d{2})-(20\d{2})', filename)
    if fy_quarter:
        fy_quarter = fy_quarter.group(1)
    else:
        fy_quarter = "Unknown"
    if fy:
        fy = fy.group(1) + "_" + fy.group(2)
    else:
        fy = re.search(r'20(\d{2})(\d{2})', filename)
        if fy:
            fy = "20" + fy.group(1) + "_" + "20" + fy.group(2)
        else:
            fy = "Unknown"
    return fy, fy_quarter

def write_ico_data_to_one_file(source_root="self-reported-personal-data-breach-cases"):
    """
    Write all downloaded ICO data to a single file.
    """
    source_directory = source_root.replace("-", "_")

    files = glob.glob(f"data/{source_directory}/*.csv")
    dataframes = []

    with open("data/column_changes.json", "r") as f:
        column_changes = json.load(f)
        column_renames = dict(column_changes.get("renames", {}))
        column_removals = column_changes.get("removals", [])

    for file in files:
        try:
            df = pd.read_csv(file, low_memory=False, encoding_errors='backslashreplace')
        except UnicodeDecodeError as e:
            print("Error decoding file:", file)
            print(e)
            return
        #standardise column names
        df.rename(columns=lambda x: x.strip().lower().replace(" ", "_").replace("-", "_"), inplace=True)

        # rename columns
        df.rename(columns=column_renames, inplace=True)
        
        # remove unwanted columns
        df.drop(columns=column_removals, inplace=True, errors='ignore')

        dataframes.append(df)
    for df in dataframes[1:]:
        if list(df.columns) != list(dataframes[0].columns):
            print("Columns do not match in file:", file)
            print("First file columns:", dataframes[0].columns)
            print("Current file columns:", df.columns)
            return

    combined_df = pd.concat(dataframes, ignore_index=True).drop_duplicates(subset=["iso_case_reference"])
    combined_df.to_csv(f"data/{source_directory}.csv", index=False)

fetch_ico_data("self-reported-personal-data-breach-cases")
fetch_ico_data("data-protection-complaints")
write_ico_data_to_one_file("self-reported-personal-data-breach-cases")
write_ico_data_to_one_file("data-protection-complaints")
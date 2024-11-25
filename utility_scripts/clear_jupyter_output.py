import json
import os

def clean_jupyter_file(filepath: str):
    print(f"Cleaning: {filepath}")
    with open(filepath) as file_in:
        data = json.load(file_in)
    data["metadata"] = {}
    for cell_index, cell in enumerate(data['cells']):
        if "execution_count" in cell.keys():
            data['cells'][cell_index]['execution_count'] = None
        if "outputs" in cell.keys():
            data['cells'][cell_index]['outputs'] = []
    with open(filepath, "w") as file_out:
        json.dump(data, file_out, indent=4)

def get_all_jupyter_files(filepath: str):
    jupyter_files = []
    for root, dir, files in os.walk(filepath):
        if ".ipynb_checkpoints" in root:
            continue
        if "_VirEnv" in dir:
            continue
        for file in files:
            if file.endswith('.ipynb'):
                jupyter_files.append(os.path.join(root, file))
    return jupyter_files

def clean_all_jupyter_files(filepath: str):
    jupyter_files = get_all_jupyter_files(filepath)
    for jupyter_file in jupyter_files:
        clean_jupyter_file(jupyter_file)

if __name__ == "__main__":
    clean_all_jupyter_files(".")

import os
from typing import List, Dict, Tuple

# Define the paths to the two server folders
folder1_path: str = r'X:\LTCC\DSP\DSP_Print\Scripts\tmep'
folder2_path: str = r'\\10.225.43.45\prod-critical\LTCC\DSP\DSP_Print\BatchLogs'

# Define the path for the output .ini file
output_file_path: str = 'differences_output.ini'

def get_ini_files(folder_path: str) -> List[Tuple[str, str]]:
    """
    Get a list of ini file paths in the given folder.
    """
    ini_files: List[Tuple[str, str]] = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.ini'):
                relative_path: str = os.path.relpath(os.path.join(root, file), folder_path)
                ini_files.append((relative_path, os.path.join(root, file)))
    return ini_files

def compare_files(file1: str, file2: str) -> List[int]:
    """
    Compare two ini files line by line.
    Return a list of line numbers where the files differ.
    """
    differences: List[int] = []
    with open(file1, 'r') as f1, open(file2, 'r') as f2:
        f1_lines: List[str] = f1.readlines()
        f2_lines: List[str] = f2.readlines()
        
        max_lines: int = max(len(f1_lines), len(f2_lines))
        for i in range(max_lines):
            line1: str = f1_lines[i] if i < len(f1_lines) else ''
            line2: str = f2_lines[i] if i < len(f2_lines) else ''
            if line1 != line2:
                differences.append(i + 1)
    return differences

def main(folder1_path: str, folder2_path: str, output_file_path: str) -> None:
    folder1_files: List[Tuple[str, str]] = get_ini_files(folder1_path)
    folder2_files: List[Tuple[str, str]] = get_ini_files(folder2_path)

    # Create a dictionary for quick access by relative path
    folder2_files_dict: Dict[str, str] = {relative_path: full_path for relative_path, full_path in folder2_files}

    differences_found: bool = False
    output_lines: List[str] = []

    for relative_path, file1 in folder1_files:
        file2: str | None = folder2_files_dict.get(relative_path)
        
        if file2:
            differences: List[int] = compare_files(file1, file2)
            if differences:
                differences_found = True
                output_lines.append(f"[{relative_path}]")
                for line_num in differences:
                    output_lines.append(f"Line {line_num}")
                output_lines.append("")  # Add an empty line for better readability

    if differences_found:
        with open(output_file_path, 'w') as output_file:
            output_file.write("\n".join(output_lines))

if __name__ == "__main__":
    main(folder1_path, folder2_path, output_file_path)

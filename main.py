import os
import sys

def clean_output_directory():
    output_dir = os.path.abspath('data/output')

    # Iterate through all files in output directory and subdirectories
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            # Check if the file ends with .txt extension
            if file.endswith(".txt"):
                # Construct the absolute path to the file and remove it
                file_path = os.path.join(root, file)
                os.remove(file_path)

if __name__ == "__main__":
    # Clean the output directory
    clean_output_directory()

    # Add Luigi directory to the Python path
    luigi_dir = os.path.abspath('Luigi')
    sys.path.append(luigi_dir)

    # Import the required Luigi module and run the task
    from luigi.cmdline import luigi_run
    sys.argv = ['luigi', '--module', 'src.tasks.mapReduce', 'MapReduce', '--local-scheduler']
    luigi_run(sys.argv)

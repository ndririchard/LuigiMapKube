import os
import json
import luigi
from b_Mapper import Mapper


class Shuffler(luigi.Task):
    id = luigi.IntParameter(default=0)
    config = luigi.Parameter(
        default=json.load(open("src/utils/luigi.json", "r"))
        )

    def requires(self):
        return (Mapper(id=i) for i in range(self.config["DEFAULT"]["NUMBER_OF_MAPPER"]))
    
    def output(self):
        return luigi.LocalTarget(os.path.join(
            self.config["SHUFFLER"]["SHUFFLER_OUTPUT_DIR"], f"shuffler_output_{self.task_id}.txt"))
    
    def defineGoals(self, config):
        distinct_keys = set()
        for filename in os.listdir(config["MAPPER"]["MAPPER_OUTPUT_DIR"]):
            filepath = os.path.join(config["MAPPER"]["MAPPER_OUTPUT_DIR"], filename)
            if os.path.isfile(filepath):
                with open(filepath, 'r') as file:
                    for line in file:
                        key_value = line.strip().split(':', 1)  # Split the line into key and value
                        if len(key_value) == 2:  # Check if the line is in the expected format
                            key = key_value[0].strip()  # Get the key by removing spaces
                            distinct_keys.add(key)  # Add the key to the set of distinct keys 
        return list(distinct_keys)
    

    def run(self):
        goal = self.defineGoals(self.config)[self.id]
        # Create the output file path
        output_file_path = self.output().path
        # Open the output file in write mode
        with open(output_file_path, 'w') as output_file:
            # Loop through each file in the input directory
            for filename in os.listdir(self.config["MAPPER"]["MAPPER_OUTPUT_DIR"]):
                input_file_path = os.path.join(self.config["MAPPER"]["MAPPER_OUTPUT_DIR"], filename)
                # Check if the file is a regular file
                if os.path.isfile(input_file_path):
                    # Read each line from the input file
                    with open(input_file_path, 'r') as input_file:
                        for line in input_file:
                            # Check if the word is present in the line
                            if line.startswith(f"{goal} :"):
                                # Write the line containing the word to the output file
                                output_file.write(line)


if __name__ == "__main__":
    luigi.build([Shuffler()], local_scheduler=True)
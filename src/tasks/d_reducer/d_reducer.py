import os
import json
import luigi
from tasks.c_shuffler.c_shuffler import Shuffler


class Reducer(luigi.Task):
    id = luigi.IntParameter()
    config = luigi.IntParameter(default=json.load(open("Config.json", "r")))

    def requires(self):
        shuf = []
        id = -1
        while id != len(self.defineGoals(self.config))-1:
            id += 1
            shuf.append(Shuffler(id=id))
        return shuf
    
    def output(self):
        # Specify the output file using the task ID
        return luigi.LocalTarget(os.path.join(
            self.config["REDUCER"]["REDUCER_OUTPUT_DIR"], f"output_reducer_{self.task_id}.txt"))
    
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
        files = os.listdir(self.config["SHUFFLER"]["SHUFFLER_OUTPUT_DIR"])    
        num_reducers = self.config["DEFAULT"]["NUMBER_OF_REDUCER"]
        list_file = [files[i::num_reducers] for i in range(num_reducers)][self.id]
        list_file=[f'{self.config["SHUFFLER"]["SHUFFLER_OUTPUT_DIR"]}/{file}' for file in list_file]
        
        # Dictionary to store the sum of values for each key
        key_value_sum = {}

        # Iterate over each file in the list
        for file_path in list_file:
            # Check if the file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File {file_path} not found.")
            # Read the content of the file and update the dictionary
            with open(file_path, 'r') as file:
                for line in file:
                    # Split the line into key and value parts
                    parts = line.strip().split(':')
                    if len(parts) != 2:
                        raise ValueError(f"Invalid format in file {file_path}: {line}")
                    key, value = parts
                    key = key.strip()  # Remove spaces around the key
                    try:
                        value = int(value.strip())  # Convert value to integer
                    except ValueError:
                        raise ValueError(f"Invalid value in file {file_path}: {value}")
                    # Update the sum of values for the key
                    key_value_sum[key] = key_value_sum.get(key, 0) + value

        # Write the result to a text file in the output directory
        output_file_path = self.output().path
        with open(output_file_path, 'w') as output_file:
            for key, value in key_value_sum.items():
                output_file.write(f"{key} : {value}\n")

if __name__ == "__main__":
    luigi.build([Reducer(id=0)], local_scheduler=True)

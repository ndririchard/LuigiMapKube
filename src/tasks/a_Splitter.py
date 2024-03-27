import os
import json
import luigi

class Splitter(luigi.Task):
    config = luigi.Parameter(
        default=json.load(open("src/utils/luigi.json", "r"))
        )
    
    def output(self):
        for i in range(self.config["DEFAULT"]["NUMBER_OF_MAPPER"]):
            yield luigi.LocalTarget(
                os.path.join(self.config["SPLITTER"]["SPLITTER_OUTPUT_DIR"], 
                             f"output_splitter_{i+1}.txt")      
                        )
    
    def run(self):
        with open(self.config["DEFAULT"]["FILE_PATH"], 'r', encoding='utf-8') as input_file:
            lines = input_file.readlines()
            total_lines = len(lines)
            
            lines_per_chunk = total_lines // self.config["DEFAULT"]["NUMBER_OF_MAPPER"]
            remainder_lines = total_lines % self.config["DEFAULT"]["NUMBER_OF_MAPPER"]

            for i in range(self.config["DEFAULT"]["NUMBER_OF_MAPPER"]):
                start_idx = i * lines_per_chunk
                end_idx = (i + 1) * lines_per_chunk if i < self.config["DEFAULT"]["NUMBER_OF_MAPPER"] - 1 else total_lines

                # Handle remainder lines
                if i == self.config["DEFAULT"]["NUMBER_OF_MAPPER"] - 1:
                    end_idx += remainder_lines

                # Write the chunk to a new file
                output_file = os.path.join(self.config["SPLITTER"]["SPLITTER_OUTPUT_DIR"], f"output_splitter_{i+1}.txt")
                with open(output_file, 'w') as output:
                    output.writelines(lines[start_idx:end_idx])

if __name__ == "__main__":
    luigi.build([Splitter()], local_scheduler=True)
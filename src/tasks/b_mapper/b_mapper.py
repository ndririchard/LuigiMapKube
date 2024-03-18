import os
import json
import luigi
import string

from tasks.a_splitter.a_splitter import Splitter

class Mapper(luigi.Task):
    id = luigi.IntParameter()
    config = luigi.IntParameter(default=json.load(open("Config.json", "r")))

    def requires(self):
        return Splitter()

    def output(self):
        return luigi.LocalTarget(os.path.join(
            self.config["MAPPER"]["MAPPER_OUTPUT_DIR"], f"output_mapper_{self.task_id}.txt"))

    def run(self):
        for index, chunk_file in enumerate(self.input()):
            if index == self.id:
                break
        chunk_file = chunk_file.path
        word_count = {}

        # Read the text file and count occurrences of each word
        with open(chunk_file, 'r') as file:
            for line in file:
                # Remove punctuation except hyphen
                translator = str.maketrans('', '', string.punctuation.replace('-', ''))
                line = line.translate(translator)

                # Splitting words in each line
                words = line.strip().split()

                # Update word count
                for word in words:
                    word = word.lower()  # Convert to lowercase for case-insensitive counting
                    if word in word_count:
                        word_count[word] += 1
                    else:
                        word_count[word] = 1

        # Write the result to the output target
        with self.output().open('w') as out:
            for word, count in word_count.items():
                out.write(f"{word} : {count}\n")

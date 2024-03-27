import os
import json
import luigi
import string

from a_Splitter import Splitter 


class Mapper(luigi.Task):
    id = luigi.IntParameter(default=0)
    config = luigi.Parameter(default=json.load(open("src/utils/luigi.json", "r")))

    def requires(self):
        return Splitter()

    def output(self):
        output_dir = self.config["MAPPER"]["MAPPER_OUTPUT_DIR"]
        return luigi.LocalTarget(os.path.join(output_dir, f"output_mapper_{self.id}.txt"))

    def run(self):
        chunk_file = None
        for index, input_file in enumerate(self.input()):
            if index == self.id:
                chunk_file = input_file.path
                break

        if not chunk_file:
            raise Exception(f"Chunk with ID {self.id} not found.")

        word_count = {}
        with open(chunk_file, 'r') as file:
            for line in file:
                translator = str.maketrans('', '', string.punctuation.replace('-', ''))
                line = line.translate(translator)
                words = line.strip().split()
                for word in words:
                    word = word.lower()
                    word_count[word] = word_count.get(word, 0) + 1

        with self.output().open('w') as out:
            for word, count in word_count.items():
                out.write(f"{word} : {count}\n")




if __name__ == "__main__":
    luigi.build([Mapper()], local_scheduler=True)

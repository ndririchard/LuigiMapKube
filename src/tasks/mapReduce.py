import os
import json
import luigi
import matplotlib.pyplot as plt
import networkx as nx
from d_reducer.d_reducer import Reducer


class MapReduce(luigi.Task):
    config = json.load(open("Config.json", "r"))

    def requires(self):
        return [Reducer(id=i) for i in range(self.config["DEFAULT"]["NUMBER_OF_REDUCER"])]
    
    def output(self):
        return luigi.LocalTarget(os.path.join(
            self.config["DEFAULT"]["OUTPUT_PATH"], f"output_{self.task_id}.txt"))
    
    def merge(self, list_file):
        key_value_sum = {}
        for file_path in list_file:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File {file_path} not found.")
            with open(file_path, 'r') as file:
                for line in file:
                    parts = line.strip().split(':')
                    if len(parts) != 2:
                        raise ValueError(f"Invalid format in file {file_path}: {line}")
                    key, value = parts
                    key = key.strip()
                    try:
                        value = int(value.strip())
                    except ValueError:
                        raise ValueError(f"Invalid value in file {file_path}: {value}")
                    key_value_sum[key] = key_value_sum.get(key, 0) + value
        return key_value_sum
    
    def draw(self, data):
        G = nx.Graph()

        for key, value in data.items():
            G.add_node(key, size=value)

        plt.figure(figsize=(12, 8))

        pos = nx.spring_layout(G)

        node_sizes = [value * 100 for value in data.values()]
        nx.draw_networkx_nodes(G, pos, node_color='skyblue', node_size=node_sizes)
        nx.draw_networkx_labels(G, pos, font_size=12)

        plt.title('Word Frequency Tree')
        plt.axis('off')
        plt.show()
    
    def run(self):
        f_path = self.config["REDUCER"]["REDUCER_OUTPUT_DIR"]
        files = [f'{f_path}/{file}' for file in os.listdir(f_path)]
        key_value_sum = self.merge(files)
        output_file_path = self.output().path
        with open(output_file_path, 'w') as output_file:
            for key, value in key_value_sum.items():
                output_file.write(f"{key} : {value}\n")
        
        # Draw the tree
        self.draw(key_value_sum)


if __name__ == "__main__":
    luigi.build([MapReduce()], local_scheduler=True)

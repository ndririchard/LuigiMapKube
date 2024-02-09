import luigi
from map_task import MapTask
from reduce_task import ReduceTask

class CollectorTask(luigi.Task):
    # Define parameter for the Collector task
    factor = luigi.IntParameter(always_in_help=True)

    # Define the requires method to specify dependencies on Mapper and Reducer tasks
    def requires(self):
        # Initialize dependencies dictionary
        deps = {
            'mapper': Mapper(factor=self.factor),
            'reducers': []
        }
        # Add Reducer tasks to dependencies list
        for i in range(self.factor):
            dep = Reducer(myid=i)
            deps['reducers'].append(dep)
        return deps

    # Define the output method to return the output target
    def output(self):
        fname = 'd-solution.txt'
        return luigi.LocalTarget(fname)

    # Define the run method to collect results from Reducer tasks and find the maximum
    def run(self):
        maximum = 0
        # Iterate over Reducer tasks and find the maximum value
        for task_input in self.input()['reducers']:
            with task_input.open('r') as f:
                num = int(f.readline().strip())
                maximum = max(maximum, num)
        # Write the maximum value to the output file
        with self.output().open('w') as myoutput:
            myoutput.write("{}\n".format(maximum))

# Entry point for running Luigi tasks
if __name__ == '__main__':
    luigi.run()

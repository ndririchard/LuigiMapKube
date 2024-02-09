import luigi
from transformer_task import Transformer

class ReduceTask(luigi.Task):
    # Define parameter for the Reducer task
    myid = luigi.IntParameter()

    # Define the requires method to specify the dependency on the Transformer task
    def requires(self):
        return Transformer(myid=self.myid)

    # Define the output method to return the output target
    def output(self):
        fname = 'c-solved-{0:02d}.txt'.format(self.myid)
        return luigi.LocalTarget(fname)

    # Define the run method to perform the reduction operation
    def run(self):
        with open(self.input().path, 'rb') as myinput, self.output().open('w') as myoutput:
            maximum = 0
            # Read input data and find the maximum value
            for num in myinput:
                maximum = max(maximum, int(num))
            # Write the maximum value to the output file
            myoutput.write("{}\n".format(maximum))

# Entry point for running Luigi tasks
if __name__ == '__main__':
    luigi.run()

import luigi
import csv

from map_task import WaiterTask

class TransformerTask(luigi.Task):
    # Define parameter for the Transformer task
    myid = luigi.IntParameter()

    # Define the requires method to specify the dependency on the Waiter task
    def requires(self):
        return WaiterTask(myid=self.myid)

    # Define the output method to return the output target
    def output(self):
        fname = 'b-transformed-{0:02d}.txt'.format(self.myid)
        return luigi.LocalTarget(fname)

    # Define the run method to perform the transformation operation
    def run(self):
        with open(self.input().path, 'rb') as myinput, self.output().open('w') as myoutput:
            # Read input data using CSV reader and write transformed data
            csvreader = csv.reader(myinput)
            for num, _, _ in csvreader:
                myoutput.write(num)
                myoutput.write('\n')

# Entry point for running Luigi tasks
if __name__ == '__main__':
    luigi.run()

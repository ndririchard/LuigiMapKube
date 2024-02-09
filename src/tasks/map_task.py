import luigi
import math
import time

class MapTask(luigi.Task):
    # Define parameters for the MapTask task
    infile = luigi.Parameter()
    factor = luigi.IntParameter()

    # Define the output method to return a list of output targets
    def output(self):
        outputs = []
        for i in range(self.factor):
            fname = 'a-part-{0:02d}.txt'.format(i)
            out = luigi.LocalTarget(fname)
            outputs.append(out)
        return outputs

    # Define the run method to perform the mapping operation
    def run(self):
        # Count the number of lines in the input file
        num_lines = sum(1 for line in open(self.infile, 'rb'))
        bulk_size = int(math.ceil(float(num_lines) / self.factor))

        # Split the input file into partitions and write to output files
        with open(self.infile, 'rb') as myinput:
            index = 0
            line_num = 0
            myoutput = self.output()[index].open('w')
            for line in myinput:
                if int(line_num / bulk_size) > index:
                    myoutput.close()
                    index += 1
                    myoutput = self.output()[index].open('w')
                myoutput.write(line)
                line_num += 1
            myoutput.close()

        # Create empty output files for remaining partitions
        for i in range(index + 1, self.factor):
            with self.output()[i].open('w') as emptyout:
                pass

class Waiter(luigi.Task):
    # Define parameter for the Waiter task
    myid = luigi.IntParameter()

    # Define the output method to return the output target
    def output(self):
        fname = 'a-part-{0:02d}.txt'.format(self.myid)
        return luigi.LocalTarget(fname)

    # Define the run method to wait for the Mapper task to finish
    def run(self):
        while True:
            if self.output().exists():
                break
            time.sleep(1)

# Entry point for running Luigi tasks
if __name__ == '__main__':
    luigi.run()

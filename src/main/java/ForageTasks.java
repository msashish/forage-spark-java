import java.io.IOException;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class ForageTasks implements Runnable {

    @Option(names = {"-f","--file"}, required=true, description="Path of input file to be processed")
    String input_file;

    public void run(){
        SparkTasks tasks = new SparkTasks(input_file);

        tasks.ProcessDataFrame();

        try {
            tasks.ProcessRDD();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException
    {
        CommandLine.run(new ForageTasks(), args);
    }

}

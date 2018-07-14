package org.carleton.JoinPattern;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.carleton.JoinPattern.Events.Event;
import org.carleton.JoinPattern.Gens.EventGenerator;
import java.awt.*;
import java.io.*;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class run {

    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";

    // setting the input variables for RR sensor
    public static Integer input_rate_A_Sec          = 10 ;        // in events/second
    public static Integer Stream_A_RunTime_S        = 1 * 10 ;       // in seconds
    public static int parallelism_for_stream_A      = 1 ;

    // setting the input stream for qrs  sensor
    public static Integer input_rate_B_Sec          = 10 ;        // in events/second
    public static Integer Stream_B_RunTime_S        = 1 * 10 ;       // in seconds
    public static int parallelism_for_stream_B      = 1 ;


    public static int WindowLength_join__ms   = 2000  ; // in ms
    public static int Slide_Interval__ms      = 100  ; // in ms

    // for complete description
    public static boolean writeMode_to_file = false;
    public static boolean appendFile        = false;
    public static boolean print_mode        = true ;

    // derived input
    public static Integer Total_Number_Of_Events_in_A = Stream_A_RunTime_S * input_rate_A_Sec * parallelism_for_stream_A;
    public static Integer Total_Number_Of_Events_in_B = Stream_B_RunTime_S * input_rate_B_Sec * parallelism_for_stream_B;

    // initializing some fields
    public static  Integer TotalNumberofComplexEvents  = 0;
    public static  Long Total_CE_Latency_ms            = 0L ;
    public static boolean is_job_finished  = false;
    public static float TotalJobRunTimeinMS ;
    public static float Engine_Runtime_S  ;
    public static float netEngine_Runtime_S  ;
    public static float  Engine_Waiting_time_S ;
    public static int number_of_joined_events ;
    public static int number_of_merged_events ;
    public static float selectivity_merged_events ;


    public static int  value ;
    public static void main(String[] args)  throws Exception {

        if(args.length == 1){
            WindowLength_join__ms = Integer.parseInt(args[0]);
        }

        // giving the conf path
        String confFile =  "/usr/local/Cellar/apache-flink/1.3.2/libexec/conf/flink-conf.yaml";

        // reading the conf file to parameter tool
        ParameterTool parameterTool  = ParameterTool.fromPropertiesFile(confFile);

         // getting the commplete configuration
        parameterTool.getConfiguration();  // this will make use flink program the required conf

        System.out.println("taskmanager.heap.mb = " + parameterTool.get("taskmanager.heap.mb"));

        //setting the envrionment variable as StreamExecutionEnvironment
        StreamExecutionEnvironment envrionment = StreamExecutionEnvironment.getExecutionEnvironment();
        envrionment.setParallelism(1);
        ExecutionConfig executionConfig = envrionment.getConfig();


        // make parameters available in the web interface
        executionConfig.setGlobalJobParameters(parameterTool);
        executionConfig.setLatencyTrackingInterval(1000);
        executionConfig.setExecutionMode(ExecutionMode.PIPELINED);
        System.out.println(" ExecutionMode = " + executionConfig.getExecutionMode());
        System.out.println("CheckpointingMode = "  + envrionment.getCheckpointingMode());

        envrionment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        System.out.println("time used is  = " + envrionment.getStreamTimeCharacteristic());


        //fetching IP of this machine
        InetAddress inetAddress = InetAddress.getLocalHost();
        String ServerIP         = inetAddress.getHostAddress();
        System.out.println("\n server IP address = " + ServerIP);


        // getting a general Stream
        DataStream<Event> stream1  = envrionment.
                addSource(new EventGenerator(input_rate_A_Sec,Total_Number_Of_Events_in_B,1,10,20)).name("A stream").setParallelism(parallelism_for_stream_A);

        // getting a general Stream
        DataStream<Event> stream2  = envrionment.
                addSource(new EventGenerator(input_rate_B_Sec,Total_Number_Of_Events_in_B,2,20,30)).name("B stream").setParallelism(parallelism_for_stream_B);

        DataStream<Event> mergedStream = stream1.union(stream2);
//        mergedStream.print();


        // performing some CEP on merged stream
        Pattern<Event,?> pattern = Pattern.<Event>begin("s1")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getSensor_id() == 1 && event.getValue() >= 15;
                    }
                }).next("s2").times(1)
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getSensor_id() == 2 && event.getValue() >= 25;
                    }
                }).within(Time.milliseconds(1000));

        PatternStream<Event> patternStream = CEP.pattern(mergedStream,pattern);


        DataStream<String> alertDataStream = patternStream.select(new PatternSelectFunction<Event, String>() {

            @Override
            public String select(Map<String, List<Event>> map) throws Exception {
                Event event = map.get("s2").get(0);
                TotalNumberofComplexEvents += 1;
                Long CEP_latency = System.currentTimeMillis() - event.getTime();
                System.out.println( ANSI_BLUE + "CEP event # " + TotalNumberofComplexEvents +" Ingestion Time = "+  event.getTime() +
                        " Detection Time = " + System.currentTimeMillis() + " CEP Latency = " + CEP_latency + " ms" );
                return null;
            }
        });



        //start the execution
       JobExecutionResult jobExecutionResult = envrionment.execute(" Started the execution ");

        selectivity_merged_events = (float) (number_of_merged_events/ (Total_Number_Of_Events_in_A + Total_Number_Of_Events_in_B));


        // --- performing computations -----
        // getting the job results like execution time
        is_job_finished         = jobExecutionResult.isJobExecutionResult();
        TotalJobRunTimeinMS     = jobExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS);
        Engine_Runtime_S        =   TotalJobRunTimeinMS/1000;
        netEngine_Runtime_S     = Engine_Runtime_S - (Total_CE_Latency_ms /1000);
        Engine_Waiting_time_S   =  Engine_Runtime_S -  netEngine_Runtime_S ;
        String win_length_string = String.valueOf(WindowLength_join__ms);


        if(is_job_finished){

            run stream_join = new run();
            stream_join.Write_into_console();
            stream_join.Write_into_text_file();
        } //  if(is_job_finished)



    }// main


    public static void Write_into_console(){

        if(print_mode && is_job_finished){

            System.out.println(" \n  -- System related  variables  -- ");
            System.out.println(" Input rate for stream A    = " + input_rate_A_Sec + " events/second");
            System.out.println("Stream A Runtime            = " + Stream_A_RunTime_S  + " seconds");
            System.out.println(" # raw events in stream A   = " + Total_Number_Of_Events_in_A + "\n");


            System.out.println(" Input rate for stream B    = " + input_rate_B_Sec + " events/second");
            System.out.println("Stream B Runtime            = " + Stream_B_RunTime_S  + " seconds");
            System.out.println(" # raw events in stream B   = " + Total_Number_Of_Events_in_B + "\n");

            System.out.println(" run Window length  = " + WindowLength_join__ms + " milliseconds");


            System.out.println("\n --- Output data ---  ");
            System.out.println(" Engine  Runtime                = " + Engine_Runtime_S + " seconds");
        }// if print_mode is on
    }

    public static void Write_into_text_file() throws IOException {
        if(writeMode_to_file && is_job_finished ){

            // writing data to file
            FileWriter fw = new FileWriter("/Users/amar/Documents/ThesisCode/CEP_Architectures/BasicPattern/src/main/java/org/carleton/JoinPattern/Data/out.txt", appendFile);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw);
            try
            {
                out.println(" \n \n -- Input  variables  -- \n");
                out.println(" Window length             = " + WindowLength_join__ms + " milliseconds");
                out.println(" Input rate for stream A   = " + input_rate_A_Sec + " events/second");
                out.println("Stream A Runtime           = " + Stream_A_RunTime_S  + " seconds");
                out.println(" # raw events in stream A  = " + Total_Number_Of_Events_in_A);

                out.println("\n --- Output data --- \n ");
                out.println(" Total #  of CE            = " + TotalNumberofComplexEvents);
                out.println(" Engine  Runtime           = " + Engine_Runtime_S + " seconds");

            } // try

            catch(Exception e){
                System.out.println(e.getStackTrace());
            }

            finally {
                out.close();

                // code to open file automatically
                File OutputFile = new File ("/Users/amar/Documents/ThesisCode/CEP_Architectures/BasicPattern/src/main/java/org/carleton/JoinPattern/Data/out.txt");

                if(OutputFile.exists()){
                    Desktop.getDesktop().open(OutputFile);
                }
                else {
                    System.out.println("file  does not exist or path is incorrect");
                }
            }// finally

        }// if writeMode_to_file is on
    }

} //class




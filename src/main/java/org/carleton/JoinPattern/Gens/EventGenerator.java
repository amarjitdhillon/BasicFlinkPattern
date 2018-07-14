  package org.carleton.JoinPattern.Gens;

  import org.apache.flink.api.java.tuple.Tuple3;
  import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
  import org.carleton.JoinPattern.Events.Event;
  import java.util.Random;

public class EventGenerator extends RichParallelSourceFunction<Event> {

    Integer InputRate;  // events/second
    Integer Sleeptime;
    Integer NumberOfEvents;
    Integer sensor_id;
    Integer upper_bound;
    Integer lower_bound;


    public EventGenerator(Integer inputRate, Integer numberOfEvents, Integer sensor_id, Integer lower_bound, Integer upper_bound) {
        this.InputRate = inputRate;
        Sleeptime = 1000 / InputRate;
        NumberOfEvents = numberOfEvents;
        this.sensor_id = sensor_id;
        this.upper_bound = upper_bound;
        this.lower_bound = lower_bound;
    }

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        Long currentTime;
        Integer Value;
        Integer Sensor_id;

        for (Integer i = 1; i <= NumberOfEvents; i++) {
            Random random   = new Random(System.currentTimeMillis());
            Sensor_id       = sensor_id;
            currentTime     = System.currentTimeMillis();

            // int randomNum = rand.nextInt((max - min) + 1) + min;
            Value           = lower_bound + random.nextInt((upper_bound - lower_bound) + 1);

            Event event     = new Event(Sensor_id, currentTime, Value);
            Tuple3<Integer, Long, Integer> stream = new Tuple3<>(Sensor_id, currentTime, Value);

            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(event);
            }
            Thread.sleep(Sleeptime);
        }
    }

    @Override
    public void cancel() {

    }
}

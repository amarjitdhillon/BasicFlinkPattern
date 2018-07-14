package org.carleton.JoinPattern.Events;


public class Event {

    public Integer sensor_id;
    public Long time;
    public Integer value;

    public Event(Integer sensor_id, Long time, Integer value) {
        this.sensor_id = sensor_id;
        this.time = time;
        this.value = value;
    }

    public Integer getSensor_id() {
        return sensor_id;
    }

    public void setSensor_id(Integer sensor_id) {
        this.sensor_id = sensor_id;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event)) return false;

        Event event = (Event) o;

        if (getSensor_id() != null ? !getSensor_id().equals(event.getSensor_id()) : event.getSensor_id() != null)
            return false;
        if (getTime() != null ? !getTime().equals(event.getTime()) : event.getTime() != null) return false;
        return getValue() != null ? getValue().equals(event.getValue()) : event.getValue() == null;
    }

    @Override
    public int hashCode() {
        int result = getSensor_id() != null ? getSensor_id().hashCode() : 0;
        result = 31 * result + (getTime() != null ? getTime().hashCode() : 0);
        result = 31 * result + (getValue() != null ? getValue().hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        return "Event{" +
                "sensor_id=" + sensor_id +
                ", time=" + time +
                ", value=" + value +
                '}';
    }



}


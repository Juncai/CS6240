package sorting;

//Author: Vikas Boddu
public class Value {
    int wban, date, time, temp;

    Value(int wban, int date, int time, int temp) {
        this.wban = wban;
        this.date = date;
        this.time = time;
        this.temp = temp;
    }

    Value(String wban, String date, String time, String temp) {
        this.wban = Integer.valueOf(wban);
        this.date = Integer.valueOf(date);
        this.time = Integer.valueOf(time);
        this.temp = Integer.valueOf(temp);
    }

    public int compareTo(Value otherValue) {
        return Integer.valueOf(this.temp).compareTo(Integer.valueOf(otherValue.temp));
    }
}

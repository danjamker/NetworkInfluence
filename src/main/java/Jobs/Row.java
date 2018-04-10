package Jobs;

import com.google.common.base.Objects;
import org.joda.time.DateTime;

/**
 * Created by danielkershaw on 15/01/2016.
 */
public class Row implements Comparable<Row> {

    private String word, user;
    private DateTime time;

    public Row(String word, String user, DateTime time) {
        this.word = word;
        this.user = user;
        this.time = time;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(word, user, time);
    }

    public String getWord() {

        return word;
    }

    public String getUser() {
        return user;
    }

    public DateTime getTime() {
        return time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        return Objects.equal(word, row.word) &&
                Objects.equal(user, row.user) &&
                Objects.equal(time, row.time);
    }

    @Override
    public int compareTo(Row o) {
        if (o.getTime().isBefore(this.getTime())) {
            return 1;
        } else if (o.getTime().isAfter(this.getTime())) {
            return -1;
        }else{
            return 0;
        }
    }
}

package Metrics;

import Evaluation.Performed;
import Evaluation.ROC;
import com.google.common.base.Objects;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.*;

/**
 * Created by danielkershaw on 15/01/2016.
 */
public class ResultsRow implements Serializable {

    private String word, user;
    private DateTime time;
    private Performed perfored_u;
    private Map<MetricType, TreeMap<DateTime, Float>> metrics = new EnumMap<MetricType, TreeMap<DateTime, Float>>(MetricType.class);
    private ROC value;
    private List<DateTime> times = new ArrayList<DateTime>();

    //TODO maybe add some code that will turn of the storage of intermediate results.
    public ResultsRow(String word, String user) {
        this.word = word;
        this.user = user;
    }

    public ResultsRow(String user, Performed p, String word, DateTime time) {
        this.user = user;
        this.perfored_u = p;
        this.word = word;
        this.time = time;
    }

    public ROC getValue() {
        return value;
    }

    public void setValue(ROC value) {
        this.value = value;
    }

    public DateTime getTime() {
        return time;
    }

    public void setTime(DateTime time) {
        this.time = time;
    }

    public float getP_u(MetricType mt) {
        if(metrics.containsKey(mt)) {
            if(metrics.get(mt).size() > 0) {
                return metrics.get(mt).get(metrics.get(mt).lastKey());
            }
            else{
                return 0f;
            }
        }
        else{
            return 0f;
        }
    }

    public float getP_u(MetricType mt, DateTime dt) {
        if(metrics.containsKey(mt)) {
            return metrics.get(mt).get(dt);
        }
        else{
            return 0f;
        }
    }

    public float get_Max_P_u(MetricType mt) {

        Comparator<Float> cmp = new Comparator<Float>() {
            @Override
            public int compare(Float o1, Float o2) {
                return o1.compareTo(o2);
            }
        };

        return Collections.max(metrics.get(mt).values(), cmp);
    }

    public DateTime get_Max_P_u_Time(MetricType mt) {
        float max = get_Max_P_u(mt);
        for (DateTime d : metrics.get(mt).keySet()) {
            if (metrics.get(mt).get(d).floatValue() == max) {
                return d;
            }
        }
        return new DateTime().minusYears(300);
    }



    public List<DateTime> getTimes(){
        return this.times;
    }

    public void setP_u(MetricType mt, float p_u, DateTime dt) {

        if(!this.metrics.containsKey(mt)) {
            this.metrics.put(mt, new TreeMap<DateTime, Float>());
        }
        if (!this.times.contains(dt)) {
            this.times.add(dt);
        }

        this.metrics.get(mt).put(dt, p_u);

    }

    public Performed getPerfored_u() {
        return perfored_u;
    }

    public void setPerfored_u(Performed perfored_u) {
        this.perfored_u = perfored_u;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(word, user, perfored_u);
    }

    public String getWord() {

        return word;
    }

    public String getUser() {
        return user;
    }

    Boolean containMertic(MetricType mt) {
        return this.metrics.containsKey(mt);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResultsRow row = (ResultsRow) o;
        return Objects.equal(word, row.word) &&
                Objects.equal(user, row.user) &&
                Objects.equal(perfored_u, row.perfored_u);
    }

}

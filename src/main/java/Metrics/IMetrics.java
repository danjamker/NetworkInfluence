package Metrics;

import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

/**
 * Created by danielkershaw on 16/01/2016.
 */
public interface IMetrics {

    void A_u(String u);

    void A_u(String u, Double value);

    float get_A_u(String u);

    void A_v_2_u(String u, String v);

    float get_A_v_2_u(String u, String v);

    void global_A_v_2_u(String u, String v);

    float get_A_v_or_u(String u, String v);

    float get_A_u_or_v(String u, String v);

    void A_v_2_u(String u, String v, Double value);

    void global_A_v_2_u(String u, String v, Double value);

    float get_global_A_v_2_u(String u, String v);

    float get_global_A_u_2_v(String u, String v);

    void credit_v_u(String u, String v, Double value);

    void _Tau_v_u_credit_v_u(String u, String v, Double value);

    double[] get_Tau_v_u_credit_v_u_array(String u, String v);

    float get_Tau_v_u_credit_v_u(String u, String v);

    float get_Tau_u_v_credit_u_v(String u, String v);

    void A_v_and_u(String u, String v);

    float  get_A_v_and_u(String u, String v);

    void A_v_and_u(String u, String v, Double value);

    void Tau_v_u(String u, String v, Double value);

    long get_Tau_v_u(String u, String v);

    long get_Tau_u_v(String u, String v);

    void Influence_u(String u, Double value);

    long get_Influence_u(String u);

    void commit();

    void setU(String u, boolean load);

    double[] get_credis_v_u(String u, String v);

    float get_total_credits_v_u(String u, String v);

    float get_total_credits_u_v(String u, String v);

    double[] get_credis_u_v(String u, String v);

    double[] get_Tau_u_v_credit_u_v_array(String u, String v);

    List<Tuple2<Tuple3<Metrics, String, String>, Double>> toList();

}

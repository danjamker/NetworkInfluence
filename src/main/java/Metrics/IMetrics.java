package Metrics;

/**
 * Created by danielkershaw on 16/01/2016.
 */
public interface IMetrics {

    void loadRow(String row_id);

    float get_A_u(String u);

    float get_A_v(String v);

    float get_A_v_2_u(String v, String u);

    float get_A_u_2_v(String u, String v);

    float get_A_v_or_u(String v, String u);

    float get_A_u_or_v(String u, String v);

    float get_global_A_v_2_u(String v, String u);

    float get_global_A_u_2_v(String u, String v);

    float get_A_v_and_u(String v, String u);

    float get_A_u_and_v(String u, String v);

    long get_Tau_v_u(String v, String u);

    long get_Tau_u_v(String u, String v);

    long get_Influence_u(String u);

    long get_Influence_v(String v);

    double[] get_credis_v_u(String v, String u);

    double[] get_credis_u_v(String u, String v);

    float get_total_credits_v_u(String v, String u);

    float get_total_credits_u_v(String u, String v);

    double[] get_Tau_v_u_credit_v_u_array(String v, String u);

    double[] get_Tau_u_v_credit_u_v_array(String u, String v);

    float get_Tau_v_u_credit_v_u(String u, String v);

    float get_Tau_u_v_credit_u_v(String u, String v);
}

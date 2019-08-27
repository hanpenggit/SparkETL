package cn.hanpeng;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.sql.*;

public class Test {
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        d();
    }

    public static void a() {
        String a="BUSI_NO,RPT_PROV,PERS_NO,PERS_ID,SURNAME,FIRSTNAME,ENG_NAME,VERF_NAME,CHN_NAME,GENDER,BIRTH_DATE,COUNTRY_CODE,FGN_STS,APPLY_RSN,PERS_REG_CATG,CERT_TYPE,CERT_NO,VERF_CERT_NO,CERT_VLD,FGN_TYPE,FGN_CARD_ID,ORIG_VISA_TYPE,ORIG_VISA_NO,VISA_TYPE,VISA_NO,VISA_DATE,VISA_VALID,VISA_VLD,SEQ_ID,VISA_RSTD_RSN,DATA_SRC_FLAG,INVALID_FLAG,VISA_ASSIGN_DEPT,LOAD_DT";
        String [] arr=a.split(",");
        int ix=1;
        StringBuilder sr=new StringBuilder();
        for(String i:arr){
            System.out.println("rs.getString("+ix+"));");
            ix++;
            sr.append("?,");
        }
        System.out.println(sr.toString());
    }

    public static void b() throws ClassNotFoundException, SQLException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        Connection conn= DriverManager.getConnection("jdbc:clickhouse://localhost:8123/default","default","123123");
        PreparedStatement ps = conn.prepareStatement("select count(1) from A");
        ResultSet rs = ps.executeQuery();
        if(rs.next()){
            System.out.println(rs.getInt(1));
        }
        rs.close();
        ps.close();
        conn.close();
    }

    public static void c(){
        String sql="SELECT a.WYBS,a.XM,a.ZJHM,a.ETL_TIMESTAMP FROM HANPENG.B_MS_B06_BJ_YW_T_CRJRYDK a where ETL_TIMESTAMP>=? and ETL_TIMESTAMP<?";
        System.out.println(sql.substring(sql.indexOf("SELECT")+6, sql.indexOf("FROM")));
    }

    public static void d() throws ClassNotFoundException, SQLException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        Connection conn= DriverManager.getConnection("jdbc:clickhouse://localhost:8123/default","default","123123");
        PreparedStatement ps = conn.prepareStatement("SELECT WYBS,XM,ZJHM,ETL_TIMESTAMP,LOAD_DT,SIGN,VERSION from A");
        ResultSet rs = ps.executeQuery();
        JSONObject record=new JSONObject();
        int maxVersion=-1;
        while(rs.next()){
            int version=rs.getInt(7);
            int sign=rs.getInt(6);
            if(version>maxVersion){
                maxVersion=version;
                record.put("WYBS",rs.getString(1));
                record.put("XM",rs.getString(2));
                record.put("ZJHM",rs.getString(3));
                record.put("ETL_TIMESTAMP",rs.getString(4));
                record.put("LOAD_DT",rs.getString(5));
                record.put("SIGN",sign);
                record.put("VERSION",version);
            }else if(version==maxVersion&&record.getIntValue("SIGN")==-sign){
                record.clear();
            }
        }
        rs.close();
        ps.close();
        conn.close();
        System.out.println(record.toJSONString());
    }


}

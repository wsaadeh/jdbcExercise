package application;

import db.DB;
import db.DbException;
import db.DbIntegrityException;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Program {
    public static void main(String[] args) {
        jdbc6();
    }

    public static void jdbc1() {
        Connection conn = DB.getConnection();
        DB.closeConnection();
    }

    public static void jdbc2() {
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            conn = DB.getConnection();
            st = conn.createStatement();
            rs = st.executeQuery("select * from department");

            while (rs.next()) {
                System.out.println(rs.getInt("Id") + "," + rs.getString("Name"));
            }
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
            DB.closeConnection();
        }
    }

    public static void jdbc3() {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Connection conn = null;
        PreparedStatement st = null;
        try {
            conn = DB.getConnection();
/*            st = conn.prepareStatement
                    (
                            "insert into seller "
                                    + "(name,email,birthdate,basesalary,departmentid)"
                                    + "values "
                                    + "(?,?,?,?,?)",
                            Statement.RETURN_GENERATED_KEYS
                    );
            st.setString(1,"Carl Purple");
            st.setString(2,"carl@gmail.com");
            st.setDate(3,new java.sql.Date(sdf.parse("22/04/1985").getTime()));
            st.setDouble(4,3000.0);
            st.setInt(5,4);*/

            st = conn.prepareStatement("insert into department (name) values ('D1'),('D2')",
                    Statement.RETURN_GENERATED_KEYS);

            Integer rowsAffect = st.executeUpdate();

            if (rowsAffect > 0) {
                ResultSet rs = st.getGeneratedKeys();
                while (rs.next()) {
                    int id = rs.getInt(1);
                    System.out.println("Done! Id = " + id);
                }

            } else {
                System.out.println("no rows affected!");
            }

        } catch (SQLException e) {//| ParseException
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(st);
            DB.closeConnection();
        }
    }

    public static void jdbc4() {
        Connection conn = null;
        PreparedStatement st = null;
        try {
            conn = DB.getConnection();
            st = conn.prepareStatement(
                    "update seller "
                    + "set basesalary = basesalary + ? "
                    + "where (departmentid = ?)"
            );
            st.setDouble(1,200.0);
            st.setInt(2,2);

            int rowsAffect = st.executeUpdate();

            if (rowsAffect > 0){
                System.out.println("Done! Rows affected: " + rowsAffect);
            }else{
                System.out.println("No rows affected");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }finally {
            DB.closeStatement(st);
            DB.closeConnection();
        }
    }

    public static void jdbc5(){
        Connection conn = null;
        PreparedStatement st = null;
        try {
            conn = DB.getConnection();

            st = conn.prepareStatement(
                    "delete from department "
                    + "where "
                    + "id = ?"
            );

            st.setInt(1, 2);

            int rowsAffected = st.executeUpdate();

            System.out.println("Done! Rows affected: " + rowsAffected);

        } catch (SQLException e) {
            throw new DbIntegrityException(e.getMessage());
        }finally {
            DB.closeStatement(st);
            DB.closeConnection();
        }
    }

    public static void jdbc6(){
        Connection conn = null;
        Statement st = null;
        try {
            conn = DB.getConnection();

            conn.setAutoCommit(false);

            st = conn.createStatement();

            int rows1 = st.executeUpdate("Update seller set basesalary = 2090 where departmentid = 1");

/*            int x = 1;
            if (x < 2 ){
                throw new SQLException("Fake error");
            }*/

            int rows2 = st.executeUpdate("Update seller set basesalary = 3090 where departmentid = 2");

            conn.commit();

            System.out.println("rows 1: " + rows1);
            System.out.println("rows 2: " + rows2);
        }catch (SQLException e){
            try {
                conn.rollback();
                throw new DbException("Transaction rolled back! Caused by: " + e.getMessage());
            } catch (SQLException ex) {
                throw new DbException("Error trying to rollback! Caused by: " + ex.getMessage() );
            }
        }finally {
            DB.closeStatement(st);
            DB.closeConnection();
        }
    }
}

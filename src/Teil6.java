import java.io.*;
import java.sql.*;

public class Teil6 {
    public static void main(String args[]) {

        String name = null;
        String passwd = null;
        String land, anreise, abreise;
        int ausstattung = 0;

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        Connection conn = null;
        Statement stmt = null;
        ResultSet rset = null;


        System.out.println("Benutzername: ");
        name = "dbsys15";
        System.out.println("Passwort:");
        passwd = "lolsql";

        try {

            System.out.println("Land: ");
            land = in.readLine();
            System.out.println("Anreise:");
            anreise = in.readLine();
            System.out.println("Abreise:");
            abreise = in.readLine();
            System.out.println("Ausstattung:");
            ausstattung = Integer.parseInt(in.readLine());
        } catch (IOException e) {
            System.out.println("Fehler beim Lesen der Eingabe: " + e);
            System.exit(-1);
        }


        System.out.println("");

        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());                // Treiber laden
            String url = "jdbc:oracle:thin:@oracle12c.in.htwg-konstanz.de:1521:ora12c"; // String für DB-Connection
            conn = DriverManager.getConnection(url, name, passwd);                        // Verbindung erstellen

            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);            // Transaction Isolations-Level setzen
            conn.setAutoCommit(false);                                                    // Kein automatisches Commit

            stmt = conn.createStatement();                                                // Statement-Objekt erzeugen

//            String myUpdateQuery = "INSERT INTO pers(pnr, name, jahrg, eindat, gehalt, anr) " +
//                    "VALUES('124', 'Huber', 1980, sysdate, 80000, 'K51')";                // Mitarbeiter hinzufügen
//            stmt.executeUpdate(myUpdateQuery);

            String mySelectQuery = "select ort, count(ferienwid) as anzahl_pro_stadt\n" +
                    "from dbsys26.Ferienwohnung inner join dbsys26.Adresse using(adressid)\n" +
                    "group by ort \n" +
                    "order by anzahl_pro_stadt desc; \n";
            rset = stmt.executeQuery(mySelectQuery);                                    // Query ausführen

            while (rset.next())
                System.out.println(rset.getInt("pnr") + " "
                        + rset.getString("name") + " "
                        + rset.getInt("jahrg") + " "
                        + rset.getString("eindat") + " "
                        + rset.getInt("gehalt") + " "
                        + rset.getString("beruf") + " "
                        + rset.getString("anr") + " "
                        + rset.getInt("vnr"));

//            myUpdateQuery = "DELETE FROM pers WHERE pnr = '124'";
//            stmt.executeUpdate(myUpdateQuery);                                            // Mitarbeiter wieder löschen

            stmt.close();                                                                // Verbindung trennen
            conn.commit();
            conn.close();
        } catch (SQLException se) {                                                        // SQL-Fehler abfangen
            System.out.println();
            System.out
                    .println("SQL Exception occurred while establishing connection to DBS: "
                            + se.getMessage());
            System.out.println("- SQL state  : " + se.getSQLState());
            System.out.println("- Message    : " + se.getMessage());
            System.out.println("- Vendor code: " + se.getErrorCode());
            System.out.println();
            System.out.println("EXITING WITH FAILURE ... !!!");
            System.out.println();
            try {
                conn.rollback();                                                        // Rollback durchführen
            } catch (SQLException e) {
                e.printStackTrace();
            }
            System.exit(-1);
        }
    }
}
import java.io.*;
import java.sql.*;

public class Teil6 {
    public static void main(String args[]) {

        String name = null;
        String passwd = null;
        String land = null;
        String anreise = null;
        String abreise = null;
        int ausstattung = -1;

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        Connection conn = null;
        Statement stmt = null;
        ResultSet rset = null;


        System.out.println("Benutzername: ");
        name = "dbsys26";
        System.out.println("Passwort:");
        passwd = "lolsql";
//
        try {

            System.out.println("Land: ");
            land = in.readLine();
            System.out.println("Anreise: (yy-mm-dd)");
            anreise = in.readLine();
            System.out.println("Abreise: (yy-mm-dd)");
            abreise = in.readLine();
            System.out.println("Ausstattung:");
            String tmp = in.readLine();
            if (!tmp.isEmpty()) ausstattung = Integer.parseInt(tmp);
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

            // Fw buchen
            System.out.println("Für den Zeitraum eine Ferienwohnung buchen [J/N]?");
            String buchen = "";
            while(buchen.equals("j") || buchen.equals("J") || buchen.equals("N") || buchen.equals("n")) {
                try {
                    buchen = in.readLine();
                } catch (IOException e) {
                    System.out.println("Falsche eingabe!");
                }
            }
            if (buchen.equals("j") || buchen.equals("J")) {
                System.out.println("Hast du bereits eine Account");
                String hasAccount = "";
                while(buchen.equals("j") || buchen.equals("J") || buchen.equals("N") || buchen.equals("n")) {
                    try {
                        hasAccount = in.readLine();
                    } catch (IOException e) {
                        System.out.println("Falsche eingabe!");
                    }
                }
                try {
                    if (hasAccount.equals("j") || hasAccount.equals("J")) {
                        String email = "";
                        String password = "";
                        String fw = "";
                        System.out.println("e-mail:");
                        email = in.readLine();
                        System.out.println("password:");
                        password = in.readLine();
                        System.out.println("Ferienwohnung der zu buchenden Wohnung:");
                        fw = in.readLine();
                        int kundenid = 1;
                        // 99, "2020-01-01", "2020-01-03", "Seehaus", 1  -> test eigaben
                        String myUpdateQuery = insertBuchung(anreise, abreise, fw, kundenid);          // Mitarbeiter hinzufügen
                        stmt.executeUpdate(myUpdateQuery);
                    } else {

                    }
                } catch (IOException e){
                    System.out.println("es gab einen Fehler bei der eingabe!");
                }
            }

            String myUpdateQuery = insertBuchung("2020-01-01", "2020-01-03", "Seehaus", 1);          // Mitarbeiter hinzufügen
            stmt.executeUpdate(myUpdateQuery);

            String mySelectQuery = sucheFerienWohnung(land, anreise, abreise, ausstattung);
            rset = stmt.executeQuery(mySelectQuery);                                    // Query ausführen

            while (rset.next())
                System.out.printf("%15s \t %.1f\n", rset.getString("name"), rset.getDouble("bewertung"));

//            myUpdateQuery = "DELETE FROM dbsys26.buchung WHERE buchungsid = '99'";
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

    private static String insertBuchung(String anreise, String abreise, String ferienwohnung, int kundenid) {
        return "INSERT INTO buchung (buchungsid, buchungsdatum, anreise, abreise, ferienwid, sterne, datumbewertung, rechnungsid, rechnungsdatum, rechnungsbetrag, kundenid) " +
                "VALUES (null, to_date('2020-01-14', 'yy-mm-dd'), to_date('" + anreise + "', 'yy-mm-dd'), to_date('" + abreise + "', 'yy-mm-dd'), " +
                "(select f.ferienwid from dbsys26.ferienwohnung f where f.name = '" + ferienwohnung + "'), " +
                " null, null, " +
                "rechungsseq.nextval, to_date('2015-02-10', 'yy-mm-dd'), 9999, " + kundenid + ") ";
    }

    public static String sucheFerienWohnung(String land, String anreise, String abreise, int ausstattung) {
        String ausstattungsFilter = "";
        if (ausstattung != -1) ausstattungsFilter = "    AND FA.ausstattungsid = " + ausstattung + " ";

        return "select F.name, avg(B.sterne) as bewertung " +
                "from dbsys26.Buchung B right outer join dbsys26.Ferienwohnung F using(ferienwid) " +
                "    inner join dbsys26.Adresse adr using(adressid) " +
                "    inner join dbsys26.Land L using(landid) " +
                "    inner join dbsys26.FerienAusstattung FA using(ferienwid) " +
                "where " +
                "    dbsys26.L.name = '" + land + "'  " +
                ausstattungsFilter +
                "    AND F.name not in ( " +
                "        select  F.name " +
                "        from dbsys26.Buchung B right outer join dbsys26.Ferienwohnung F using(ferienwid) " +
                "        where ((b.anreise  between to_date('" + anreise + "', 'yy-mm-dd') and to_DATE('" + abreise + "', 'yy-mm-dd'))) " +
                "        OR ((b.abreise between to_date('" + anreise + "', 'yy-mm-dd') and to_DATE('" + abreise + "', 'yy-mm-dd'))) " +
                "        or (b.anreise < to_date('" + anreise + "', 'yy-mm-dd') and b.abreise > to_DATE('" + abreise + "', 'yy-mm-dd'))) " +
                "group by F.name " +
                "order by avg(B.sterne)";
    }
}


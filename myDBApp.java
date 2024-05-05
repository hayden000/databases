package database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class myDBApp {
	public static void main(String[] argv) throws SQLException {
		System.out.println("Enter database username:");
		Scanner username = new Scanner(System.in);
		String user = username.nextLine();
		System.out.println("Enter database password:");
		Scanner passcode = new Scanner(System.in);
		String password = username.nextLine();
		username.close();
		passcode.close();
		String database = "teachdb.cs.rhul.ac.uk";
		Connection connection = connectToDatabase(user, password, database);
		if (connection != null) {
			System.out.println("SUCCESS: You made it!" + "\n\t You can now take control of your database!\n");
		} else {
			System.out.println("ERROR: \tFailed to make connection!");
			System.exit(1);
		}
		String delayedFlight = " \"ID of Delayed Flight\" int\n" + "		constraint delayedFlights_pk\n"
				+ "			primary key,\n" + "	\"Month\" int not null,\n" + "	\"DayofMonth\" int not null,\n"
				+ "	\"DayOfWeek\" int not null,\n" + "	\"DepTime\" int not null,\n"
				+ "	\"ScheduledDepTime\" int not null,\n" + "	\"ArrTime\" int not null,\n"
				+ "	\"ScheduledArrTime\" int not null,\n" + "	\"UniqueCarrier\" varchar not null,\n"
				+ "	\"FlightNum\" int not null,\n" + "	\"ActualFlightTime\" int not null,\n"
				+ "	\"scheduledFlightTime\" int not null,\n" + "	\"AirTime\" int not null,\n"
				+ "	\"ArrDelay\" int not null,\n" + "	\"DepDelay\" int not null,\n"
				+ "	\"Orig\" varchar not null,\n" + "	\"Dest\" varchar not null,\n" + "	\"Distance\" int not null, "
				+ "constraint" + '"' + "Dest" + '"' + "foreign key (" + '"' + "Orig" + '"' + ") references airport("
				+ '"' + "airportCode" + '"' + "), constraint " + '"' + "Orig" + '"' + "foreign key (" + '"' + "Dest"
				+ '"' + ") references airport(" + '"' + "airportCode" + '"' + ")";
		String airport = '"' + "airportCode" + '"' + "varchar constraint airport_pk primary key," + '"' + "airportName"
				+ '"' + " varchar not null," + '"' + "City" + '"' + " varchar not null," + '"' + "State" + '"'
				+ " varchar not null";
		drop(connection, "DelayedFlight");
		drop(connection, "airport");
		createTable(connection, "airport", airport);
		createTable(connection, "DelayedFlight", delayedFlight);
		insertAirportData(connection, "airport", "airport.txt");
		insertDelayedFlightData(connection, "DelayedFlight", "delayedFlights.txt");
		String q1 = "select distinct \"UniqueCarrier\", count(\"UniqueCarrier\") as repeat from \"DelayedFlight\" group by \"UniqueCarrier\" order by repeat DESC limit 5";
		String q2 = "select distinct \"City\", Count (\"Orig\") AS repeat from airport inner join \"DelayedFlight\" dF on airport.\"airportCode\" = dF.\"Orig\" group by \"City\" order by Count(\"Orig\") DESC limit 5;\n"
				+ "";
		String q3 = "select distinct \"Dest\", sum(\"ArrDelay\") as total from \"DelayedFlight\" group by \"Dest\" order by total DESC limit 5 offset 1";
		String q4 = "select distinct airport.\"State\" as state, Count(airport1.\"State\") as Repeat\n"
				+ "from airport as airport1\n"
				+ "inner join (airport inner join \"DelayedFlight\" on \"airportCode\" = \"DelayedFlight\".\"Orig\") on airport1.\"airportCode\" = \"DelayedFlight\".\"Dest\"\n"
				+ "group by airport.\"State\", airport1.\"State\"\n"
				+ "having (((airport.\"State\")=airport1.\"State\"))\n"
				+ "order by Count(airport1.\"State\") DESC limit 5;";
		System.out.println("");
		System.out.println("########### 1st Query ########");
		output(query(connection, q1));
		System.out.println("########### 2nd Query ########");
		output(query(connection, q2));
		System.out.println("########### 3rd Query ########");
		output(query(connection, q3));
		System.out.println("########### 4th Query ########");
		output(query(connection, q4));
	}

	public static void output(ResultSet results) throws SQLException {
		if (results.next()) {
			do {
				System.out.println(results.getString(1) + " --> " + results.getString(2));
			} while (results.next());
		}
		System.out.println("");
	}

	public static void drop(Connection connection, String tableName) {
		System.out.println("Dropping table ... " + tableName);
		try {
			char symbol = '"';
			String command = "DROP TABLE IF EXISTS " + symbol + tableName + symbol + "CASCADE";
			Statement statement = connection.createStatement();
			statement.execute(command);
			statement.close();
			System.out.println("Sucess!");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Drop operation Failed");
		}
	}

	public static void createTable(Connection connection, String tableName, String rows) {
		System.out.println("Creating table ... " + tableName);
		try {
			String command = "create table \"" + tableName + "\"\n" + "(\n" + rows + ")";
			Statement statement = connection.createStatement();
			statement.execute(command);
			statement.close();
			System.out.println("Sucess!");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Create operation Failed");
		}
	}

	public static void insertDelayedFlightData(Connection connection, String tableName, String file) {
		System.out.println("Inserting delayed flight data");
		try {
			int numRows = 18;
			String currentLine = null;
			BufferedReader br = new BufferedReader(new FileReader(file));
			Statement st = connection.createStatement();
			if ((currentLine = br.readLine()) != null) {
				do {
					String[] values = currentLine.split(",");
					String composedLine = "INSERT INTO " + '"' + tableName + '"' + " VALUES (" + values[0] + ","
							+ values[1] + "," + values[2] + "," + values[3] + "," + values[4] + "," + values[5] + ","
							+ values[6] + "," + values[7] + "," + "'" + values[8] + "'" + "," + values[9] + ","
							+ values[10] + "," + values[11] + "," + values[12] + "," + values[13] + "," + values[14]
							+ "," + "'" + values[15] + "'" + "," + "'" + values[16] + "'" + "," + values[17] + ")";
					numRows = st.executeUpdate(composedLine);
				} while ((currentLine = br.readLine()) != null);
			}
			System.out.println("Sucess!");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Insert operation Failed");
		}
	}

	public static void insertAirportData(Connection connection, String tableName, String file) {
		System.out.println("Inserting airport data");
		try {
			int numRows = 4;
			String currentLine = null;
			BufferedReader br = new BufferedReader(new FileReader(file));
			Statement st = connection.createStatement();
			if ((currentLine = br.readLine()) != null) {
				do {
					String[] values = currentLine.split(",");
					String composedLine = "INSERT INTO " + '"' + tableName + '"' + " VALUES (" + "'" + values[0] + "'"
							+ "," + "'" + values[1] + "'" + "," + "'" + values[2] + "'" + "," + "'" + values[3] + "'"
							+ ")";
					numRows = st.executeUpdate(composedLine);
				} while ((currentLine = br.readLine()) != null);
			}
			System.out.println("Sucess!");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static ResultSet query(Connection connection, String query) {
		try {
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery(query);
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static Connection connectToDatabase(String user, String password, String database) {
		System.out.println("------ Testing PostgreSQL JDBC Connection ------");
		Connection connection = null;
		try {
			String protocol = "jdbc:postgresql://";
			String dbName = "/CS2855/";
			String fullURL = protocol + database + dbName + user;
			connection = DriverManager.getConnection(fullURL, user, password);
		} catch (SQLException e) {
			String errorMsg = e.getMessage();
			if (errorMsg.contains("authentication failed")) {
				System.out.println("ERROR: \tDatabase password is incorrect.");
			} else {
				System.out.println("Connection failed!");
				e.printStackTrace();
			}
		}
		return connection;
	}
}
/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql){//1
		try{
			String querySize = String.format("SELECT id FROM Customer");
			List<List<String>> data = esql.executeQueryAndReturnResult(querySize);
			int newid = data.size() + 1;
			System.out.print("Enter new customer's first name: ");
			String newfname = in.readLine();
			System.out.print("Enter new customer's last name: ");
			String newlname = in.readLine();
			System.out.print("Enter new customer's phone number: ");
			String newphone = in.readLine();
	         	System.out.print("Enter new customer's address: ");
			String newaddress = in.readLine();

			String query = String.format("INSERT INTO Customer(id, fname, lname, phone, address) VALUES(%d, '%s', '%s', '%s', '%s')", newid,newfname,newlname,newphone,newaddress);

        		esql.executeUpdate(query);
         		//System.out.println ("total row(s): " + rowCount);
      		}catch(Exception e){
        		 System.err.println (e.getMessage());
      		}
	} //end AddCustomer
	
	public static void AddMechanic(MechanicShop esql){//2
		try{
			String querySize = String.format("SELECT id FROM Mechanic");
			List<List<String>> data = esql.executeQueryAndReturnResult(querySize);
			int newid = data.size() + 1;

			System.out.print("Enter new mechanic's first name: ");
			String newfname = in.readLine();
			System.out.print("Enter new mechanic's last name: ");
			String newlname = in.readLine();
			System.out.print("Enter new mechanic's years of experience: ");
			String tempyears = in.readLine();
			int newyear = Integer.parseInt(tempyears);

			String query = String.format("INSERT INTO mechanic(id, fname, lname, experience) VALUES(%d, '%s', '%s', %d)", newid, newfname, newlname, newyear);

			//INSERT INTO Mechanic(id, fname, lname, experience) VALUES(251, 'Thomas', 'Lee', 10);
        		esql.executeUpdate(query);
         		//System.out.println ("total row(s): " + rowCount);
      		}catch(Exception e){
        		 System.err.println (e.getMessage());
      		}
	}
	
	public static void AddCar(MechanicShop esql){//3
		try{
			System.out.print("Enter new car's VIN: ");
			String newvin = in.readLine();
			System.out.print("Enter new car's make: ");
			String newmake = in.readLine();
			System.out.print("Enter new car's model: ");
			String newmodel = in.readLine();
			System.out.print("Enter new car's year: ");
			String tempyears = in.readLine();
			int newyear = Integer.parseInt(tempyears);

			String query = String.format("INSERT INTO Car(vin, make, model, year) VALUES('%s', '%s', '%s', %d)", newvin, newmake, newmodel, newyear);

			//INSERT INTO Car(vin, make, model, year) VALUES('MYCARSVIN420', 'Lamborghini', 'Altima', 2018);

        		esql.executeUpdate(query);
         		//System.out.println ("total row(s): " + rowCount);
      		}catch(Exception e){
        		 System.err.println (e.getMessage());
      		}
	}
	
public static void InsertServiceRequest(MechanicShop esql){//4
	//TODO  
/*
Cano     
*/
		try{
			String querySize = String.format("SELECT rid FROM Service_Request");
			List<List<String>> ridsize = esql.executeQueryAndReturnResult(querySize);
			int newrid = ridsize.size() + 1;

         		System.out.print("Enter last name for lookup: ");
			String lookup = in.readLine();
			
			String query = String.format("SELECT * FROM Customer WHERE lname = '%s'", lookup);
			int rowcount = esql.executeQueryAndPrintResult(query);


			if(rowcount == 0) {
				System.out.println("Last name does not exist, would you like to add a new customer? Y/N: ");
				String yesno = in.readLine();

				if(yesno.equals("y") || yesno.equals("Y")) {
					AddCustomer(esql);
				}
				else {
					//do nothing return to menu
				}
				
				
			}
			else {
				System.out.println("Please input the id of the customer you would like to select: ");
				String tempid = in.readLine();
				int custid = Integer.parseInt(tempid);
				query = String.format("SELECT Row_Number() OVER ( ORDER BY Owns.car_vin ), Car FROM Customer,Owns,Car WHERE Customer.id = Owns.customer_id AND Car.vin = Owns.car_vin AND 					Owns.customer_id = '%s'", custid);
				rowcount = esql.executeQueryAndPrintResult(query);
				System.out.println("Please input the Row Number of the car you would like to select, or type 0 to add a new car: ");
				String temprow = in.readLine();
				int rownum = Integer.parseInt(temprow);
				
				if(rownum == 0) {
					//THIS IS ALL ADDCAR CODE
					System.out.print("Enter new car's VIN: ");
					String newvin = in.readLine();
					System.out.print("Enter new car's make: ");
					String newmake = in.readLine();
					System.out.print("Enter new car's model: ");
					String newmodel = in.readLine();
					System.out.print("Enter new car's year: ");
					String tempyears = in.readLine();
					int newyear = Integer.parseInt(tempyears);

					query = String.format("INSERT INTO Car(vin, make, model, year) VALUES('%s', '%s', '%s', %d)", newvin, newmake, newmodel, newyear);
					esql.executeUpdate(query);
					
					querySize = String.format("SELECT ownership_id FROM Owns");
					List<List<String>> ownsize = esql.executeQueryAndReturnResult(querySize);
					int newown = ownsize.size() + 1;
					query = String.format("INSERT INTO Owns(ownership_id, customer_id, car_vin) VALUES(%d, %d, '%s')", newown, custid, newvin);
					esql.executeUpdate(query);


					System.out.print("New car added to database.\n");

					//System.out.print("Enter today's date (YYYY-MM-dd): ");
					//String newdate = in.readLine();
					//int date = Integer.parseInt(newdate);
					System.out.print("Enter the current odometer reading on the car: ");
					String newodo = in.readLine();
					int odo = Integer.parseInt(newodo);
					System.out.print("Enter customer's complaints with the car: ");
					String newcomplaint = in.readLine();
					

					query = String.format("INSERT INTO Service_Request(rid, customer_id, car_vin, date, odometer, complain) VALUES(%d, %d, '%s', CURRENT_DATE, %d, '%s')", newrid, custid, newvin, odo, newcomplaint);
					esql.executeUpdate(query);
					System.out.print("Your service request id is: ");
					System.out.print(newrid);
					System.out.printf("%n"); 
				}
				else {

//					query = String.format("SELECT Row_Number() OVER ( ORDER BY Owns.car_vin ), Car FROM Customer,Owns,Car WHERE Customer.id = Owns.customer_id AND Car.vin = Owns.car_vin AND Owns.customer_id = '%s'", custid);
					query = String.format("SELECT test FROM (Select Row_Number() OVER ( ORDER BY Owns.car_vin ) as rownumber,Car.vin,Car.make,Car.model,Car.year FROM Customer,Owns,Car WHERE Customer.id = Owns.customer_id AND Car.vin = Owns.car_vin AND Owns.customer_id = '%s') AS test WHERE rownumber = %d ", custid,rownum);
					List<List<String>> result  = esql.executeQueryAndReturnResult(query);
					
					String car = result.get(0).get(0);
					System.out.println(car);
					String[] output = car.split(",");
					String newvin = output[1];
					
					System.out.print("Enter the current odometer reading on the car: ");
					String newodo = in.readLine();
					int odo = Integer.parseInt(newodo);
					System.out.print("Enter customer's complaints with the car: ");
					String newcomplaint = in.readLine();

					query = String.format("INSERT INTO Service_Request(rid, customer_id, car_vin, date, odometer, complain) VALUES(%d, %d, '%s', CURRENT_DATE, %d, '%s')", newrid, custid, newvin, odo, newcomplaint);
					esql.executeUpdate(query);
					System.out.print("Your service request id is: ");
					System.out.print(newrid);
					System.out.printf("%n"); 

				}

			}


         		
      		}catch(Exception e){
         		System.err.println (e.getMessage());
      		}

	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
	//TODO
	try{
			String querySize = String.format("SELECT wid FROM Closed_Request");
			List<List<String>> widsize = esql.executeQueryAndReturnResult(querySize);
			int newwid = widsize.size() + 1;

         		System.out.println("Enter a service request number: ");
			String temprequest = in.readLine();
			int requestnum = Integer.parseInt(temprequest);
			System.out.println("Enter your mechanic id: ");
			String tempmid = in.readLine();
			int mechanicid = Integer.parseInt(tempmid);

			System.out.println("Enter any comments you have about the repairs: ");
			String comments = in.readLine();

			System.out.println("Enter the amount to bill the customer: ");
			String tempbill = in.readLine();
			int bill = Integer.parseInt(tempbill);

			String query = String.format("SELECT * FROM Mechanic WHERE Mechanic.id = %d", mechanicid);
			int numRows = esql.executeQuery(query);
			
			if(numRows == 0) {
				System.out.println("Mechanic ID does not exist.\n");
				return;
			}
			
			query = String.format("SELECT * FROM Service_Request WHERE Service_Request.rid = %d", requestnum);
			numRows = esql.executeQuery(query);
			if(numRows == 0) {
				System.out.println("Service request number does not exist.\n");
				return;
			}

			query = String.format("SELECT * FROM Service_Request WHERE Service_Request.rid = %d AND Service_Request.date <= CURRENT_DATE", requestnum);
			numRows = esql.executeQuery(query);
			if(numRows == 0) {
				System.out.println("Service request end date exceeds the request date.\n");
				return;
			}

			query = String.format("INSERT INTO Closed_Request(wid, rid, mid, date, comment, bill) VALUES(%d, %d, %d, CURRENT_DATE, '%s', %d)", newwid, requestnum, mechanicid, comments, bill);

			esql.executeUpdate(query);

			
      		}catch(Exception e){
         		System.err.println (e.getMessage());
      		}
}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try{
         		String query = "SELECT Customer.fname, Customer.lname, Closed_Request.bill, Service_Request.date, Closed_Request.comment FROM Customer,Closed_Request,Service_Request WHERE Closed_Request.bill < 100 AND Closed_Request.rid = Service_Request.rid AND Service_Request.customer_id = Customer.id";
         		int rowCount = esql.executeQueryAndPrintResult(query);
         		System.out.println ("total row(s): " + rowCount);
      		}catch(Exception e){
         		System.err.println (e.getMessage());
      		}
	}

	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
	//TODO		
		try{
 		String query = "SELECT allCars.fname, allCars.lname, allCars.numCars FROM (SELECT Owns.customer_id, Customer.fname, Customer.lname, COUNT(*) numCars FROM Owns,Customer WHERE Customer.id = Owns.customer_id GROUP BY Owns.customer_id, Customer.fname, Customer.lname) AS allCars WHERE numCars > 20";
		
		// Test		
		//String query = "SELECT Owns.customer_id, Owns.car_vin FROM Owns WHERE Owns.customer_id = 110 OR Owns.customer_id = 28 OR Owns.customer_id = 24 ";
 		int rowCount = esql.executeQueryAndPrintResult(query);
 		System.out.println ("total row(s): " + rowCount);
	}catch(Exception e){
 		System.err.println (e.getMessage());
	}

	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		//TODO Get only Make,Model,Year
		try{
         		String query = "SELECT Car.make, Car.model, Car.year, Service_Request.odometer FROM Car,Service_Request WHERE Service_Request.car_vin = Car.vin AND Service_Request.odometer < 50000 AND Car.year < 1995";
         		int rowCount = esql.executeQueryAndPrintResult(query);
         		System.out.println ("total row(s): " + rowCount);
      		}catch(Exception e){
         		System.err.println (e.getMessage());
      		}
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
	//TODO
	try{
		System.out.print("Cars should have how many Service Orders Open? ");
		String tempServ = in.readLine();
		int numServ = Integer.parseInt(tempServ);
		
		//Lists first k number of cars with most requests open
		//String query = String.format("SELECT Car.make, Car.model, Car.vin, COUNT(Service_Request) FROM Car, Service_Request WHERE Car.vin = Service_Request.car_vin GROUP BY Car.vin ORDER BY count DESC LIMIT %d", numServ);

		//Lists first k number of cars with most requests OPEN
		//String query = String.format("SELECT Car.make, Car.model, Car.vin, COUNT(Service_Request) FROM Car,Service_Request WHERE Service_Request.rid NOT IN (SELECT Service_Request.rid FROM Closed_Request,Service_Request WHERE Service_Request.rid = Closed_Request.rid) AND Car.vin = Service_Request.car_vin GROUP BY Car.vin ORDER BY count DESC LIMIT %d",numServ);

		//Lists first x cars with k requests open

		System.out.print("How many cars would you like to see ");
		tempServ = in.readLine();
		int carServ = Integer.parseInt(tempServ);

		String query = String.format("SELECT Car.make, Car.model, Car.vin, COUNT(Service_Request) as cnt FROM Car,Service_Request WHERE Service_Request.rid NOT IN (SELECT Service_Request.rid FROM Closed_Request,Service_Request WHERE Service_Request.rid = Closed_Request.rid) AND Car.vin = Service_Request.car_vin GROUP BY Car.make,Car.model,Car.vin HAVING COUNT(*) = %d ORDER BY cnt DESC LIMIT %d",numServ,carServ);

		int rowCount = esql.executeQueryAndPrintResult(query);
 		System.out.println ("total row(s): " + rowCount);
	}catch(Exception e){
 		System.err.println (e.getMessage());
	}

	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//10
	//TODO
	try{

		String query = "SELECT A.fname,A.lname, SUM(Closed_Request.bill) total_bill FROM (SELECT Customer.fname, Customer.lname, Customer.id, Closed_Request.bill, Closed_Request.rid FROM Customer, Closed_Request, Service_Request WHERE Closed_Request.rid = Service_Request.rid AND Customer.id = Service_Request.customer_id) AS A LEFT JOIN Closed_Request ON A.rid = Closed_Request.rid GROUP BY A.fname,A.lname,A.id ORDER BY total_bill DESC";



		int rowCount = esql.executeQueryAndPrintResult(query);
 		System.out.println ("total row(s): " + rowCount);
	}catch(Exception e){
 		System.err.println (e.getMessage());
	}



















	
	}
	
}

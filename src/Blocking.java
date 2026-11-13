import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.random.RandomGenerator;

/**
 * 
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 * 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
*/

public class Blocking {
	public static int num_starting_tasks = 30; // Initial starting tasks
	public static int num_queries_per_task = 3;
	
	Connection con;
	ThreadPoolExecutor executor;
	Random rand = new Random(123456);
	int num_tuple = 5000;
	double max_value = 50.0f;
	AtomicInteger tasks_finished;
	AtomicInteger current_num_tuple;
	AtomicInteger next_task;
	AtomicInteger aborted_tasks;
	volatile boolean run; // volatile needed for threads
	
	public Blocking(){
		tasks_finished = new AtomicInteger(0);
		current_num_tuple = new AtomicInteger(num_tuple);
		next_task = new AtomicInteger(0);
		aborted_tasks = new AtomicInteger(0);
		run = true;
	}
	
	void executeSQL(String sql, Connection con){
		java.sql.Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.execute(sql);
			System.out.println("Executing "+sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	void cerateDB(){	
		try {
			Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
			executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);
			
			PreparedStatement ps;
			con.setAutoCommit(false);
			ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			for(int i=0;i<num_tuple;i++){
				ps.setInt(1, i);
				ps.setDouble(2,rand.nextDouble()*max_value);
				ps.setInt(3, 0);
				ps.addBatch();
				ps.clearParameters();
			}
			
			int[] results = ps.executeBatch();
			con.commit();
			con.close();
			System.out.println(Arrays.toString(results));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	void generateNewTask() {
		if (run == false) {
			return;
		} else {
			try {
				executor.execute(new Blocker(next_task.getAndIncrement()));
			} catch (RejectedExecutionException e) {
				// do nothing, only happens on shutdown if a thread still wants to start a new execution
				// and the narrowly passes the run = false check.
			}
		}

	}

	public static void main(String[] args) throws InterruptedException {
		//Create DB
		Blocking b = new Blocking();
		b.cerateDB();

		// Start Transaction generation and execution
		b.executor =
				(ThreadPoolExecutor) Executors.newFixedThreadPool(4);//XXX - Do not change the number of threads used
		long start = System.currentTimeMillis();
		for (int i = 0; i < num_starting_tasks; i++) {
			b.generateNewTask();
		}

		Thread.sleep (600000);
		b.run = false;
		b.executor.shutdown();
		b.executor.awaitTermination(10, TimeUnit.MINUTES);
		long stop = System.currentTimeMillis();

		System.out.println("[DONE] Task execution after roughly "+((stop-start) / 1000.0)+" s. Task throughput: "+b.tasks_finished.get() / ((stop-start) / 1000.0)+
				"Tasks/s. Tasks finished: "+b.tasks_finished+", Tasks aborted: "+b.aborted_tasks);
	}
	
	/**
	 * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
	 *
	 */
	public class Blocker implements Runnable  {
		final int task;
		//final double [] start_range; // Not needed in this implementation
		//final double [] stop_range; // Not needed in this implementation
		double p;

		public Blocker(int id){
			this.task = id;
			/*
			this.start_range = new double[num_queries_per_task];
			this.stop_range  = new double[num_queries_per_task];
			for(int q=0;q<num_queries_per_task;q++) {
				stop_range[q]  = rand.nextDouble()*max_value;
				start_range[q] = rand.nextDouble()*stop_range[q];
			}
			*/
		}
		
		@Override
		public void run() {
			Connection con=null;//XXX - Yes there needs to be a separate Connection per blocking taks. Otherwise strange things can happen
			con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);//XXX This is the class you need to create, but does not have to be in the Repo
			PreparedStatement ps;
			ResultSet rs;

			RandomGenerator rng_for_p = RandomGenerator.getDefault(); // For true i.i.d behavior
			p = rng_for_p.nextInt(11) / 10.0; // Outputs numbers from 0.0 to 1.0 (inclusive)
			//System.out.println("Current p: "+p);
			try {
				if (p < 0.6) {
					// Case 1: read-only multi-point query
					con.setAutoCommit(false);
					con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					int q = 0;
					double sum = 0;
					int random_tuple = 0;
					ps = con.prepareStatement("Select data_value from blocking_data where data_id = ?");

					while (q < num_queries_per_task) {
						random_tuple = ThreadLocalRandom.current().nextInt(current_num_tuple.get());
						ps.setInt(1, random_tuple);
						rs = ps.executeQuery();
						if (rs.next()) {
							sum = sum + rs.getDouble("data_value");
						}
						rs.close();
						q++;
					}

					con.commit();
					int num_finished = tasks_finished.incrementAndGet();
					System.out.println("Done task " + task + ", Read-only multi-point query. I am finisher number " + num_finished + ". My sum is: " + sum);
				} else if (p >= 0.6 && p < 0.7) {
					//Case 2: new tuple creation
					con.setAutoCommit(false);
					con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

					ps.setInt(1, current_num_tuple.get());
					ps.setDouble(2, rand.nextDouble() * max_value);
					ps.setInt(3, task);
					ps.executeUpdate();
					ps.clearParameters();

					con.commit();
					int num_finished = tasks_finished.incrementAndGet();
					current_num_tuple.incrementAndGet();
					System.out.println("Done task " + task + ", Insert tuple into DB with ID " + (current_num_tuple.get() - 1) + ". I am finisher number " + num_finished);
				} else if (p >= 0.7 && p < 0.8) {
					//Case 3: full table read-only scan
					con.setAutoCommit(false);
					con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					double sum = 0;
					ps = con.prepareStatement("Select sum(data_value) from blocking_data");
					rs = ps.executeQuery();
					if (rs.next()) {
						sum = sum + rs.getDouble("sum");
					}
					rs.close();

					con.commit();
					int num_finished = tasks_finished.incrementAndGet();
					System.out.println("Done task " + task + ", Full table scan and sum. I am finisher number " + num_finished + ". My sum is: " + sum);
				} else if (p >= 0.8) {
					// Case 4: write query with (num_queries_per_task) 3 tuple update statements
					con.setAutoCommit(false);
					con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					int q = 0;
					int random_tuple = 0;
					ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?;", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

					while (q < num_queries_per_task) {//I want to execute num_queries_per_task update queries
						random_tuple = ThreadLocalRandom.current().nextInt(current_num_tuple.get());
						ps.setDouble(1, rand.nextDouble() * max_value); //data_value
						ps.setDouble(2, task); // modified_by
						ps.setDouble(3, random_tuple); //data_id
						//System.out.println("Started query "+q+" task "+task+" Executing "+ps); // debugging
						ps.executeUpdate();
						q++;
					}

					con.commit();
					int num_finished = tasks_finished.incrementAndGet();
					System.out.println("Done task " + task + ", Write Query with update. I am finisher number " + num_finished);
				}
			} catch (SQLException e) {
				aborted_tasks.incrementAndGet();
				try {
					con.rollback(); // release lock immediately
				} catch (SQLException e2) {
					e2.printStackTrace();
				}
			} finally { // Con close and queue new tasks
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				generateNewTask();
			}
		}
	}
}

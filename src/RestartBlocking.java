import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

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

public class RestartBlocking {
	public static int num_taks=30;
	public static int num_queries_per_task = 3;
	private static final String DEADLOCK_CODE = "40P01";//PostgreSQL SQLSTATE for "deadlock detected"
	
	Connection con;
	Random rand = new Random(123456);
	int num_tuple = 50000;
	double max_value = 50.0f;
	AtomicInteger tasks_finished;
	AtomicInteger tasks_restarted;
	
	public RestartBlocking(){
		tasks_finished = new AtomicInteger(0);
		tasks_restarted = new AtomicInteger(0);
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
	
	public static void main(String[] args){
		//Create DB
		RestartBlocking b = new RestartBlocking();
		b.cerateDB();
		
		//Create task and their queries
		ArrayList<RestartBlocking.Blocker> my_tasks = new ArrayList<RestartBlocking.Blocker>(num_taks);
		for(int task=0;task<num_taks;task++){
			my_tasks.add(b.new Blocker(task));
		}
		
		//Execute tasks
		ThreadPoolExecutor executor = 
				  (ThreadPoolExecutor) Executors.newFixedThreadPool(4);//XXX - Do not change the number of threads used
		double start, stop;
		start = System.currentTimeMillis();
		for(Blocker task : my_tasks) {
			executor.execute(task);
		}
		executor.shutdown();
		
		//wait for the tasks to finish
		while(!executor.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		stop = System.currentTimeMillis();
		System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+b.tasks_finished+" of "+num_taks+", restarts: "+b.tasks_restarted);
	}
	
	/**
	 * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
	 *
	 */
	public class Blocker implements Runnable  {
		final int task;
		final double [] start_range;
		final double [] stop_range;
		
		public Blocker(int id){
			this.task = id;
			this.start_range = new double[num_queries_per_task];
			this.stop_range  = new double[num_queries_per_task];
			for(int q=0;q<num_queries_per_task;q++) {
				stop_range[q]  = rand.nextDouble()*max_value;
				start_range[q] = rand.nextDouble()*stop_range[q];
			}
		}
		
		@Override
		public void run() {
			Connection con=null;//XXX - Yes there needs to be a separate Connection per blocking taks. Otherwise strange things can happen
			con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);//XXX This is the class you need to create, but does not have to be in the Repo
			PreparedStatement ps;
			boolean committed = false;
			int attempt = 1;
			
			while(!committed) {//restart the transaction until it commits
				try {
					con.setAutoCommit(false);
					con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					int q = 0;//always restart with the first query, the rollback undid all of them
					ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?,  modified_by = "+task+" WHERE data_value >= ? AND data_value <= ?;",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
				
					while(q<num_queries_per_task) {//I want to execute num_queries_per_task update queries
						ps.setDouble(1, (float) (rand.nextDouble()*max_value));//some value, which one is not important.
						ps.setDouble(2, start_range[q]);
						ps.setDouble(3, stop_range[q]);
						//System.out.println("Started query "+q+" task "+task+" Executing "+ps);
						ps.execute();
						q++;
					}
					con.commit();
					committed = true;
					int num_finished = tasks_finished.incrementAndGet();
					System.out.println("Done task "+task+" after "+attempt+" attempt(s). I am finisher number "+num_finished);
				} catch (SQLException e) {
					if (DEADLOCK_CODE.equals(e.getSQLState())) {//we were chosen as deadlock victim -> roll back and try again
						tasks_restarted.incrementAndGet();
						attempt++;
						System.out.println("Task "+task+" deadlock -> restart");
						try {
							con.rollback();
						} catch (SQLException ex) {
							throw new RuntimeException(ex);
						}
					} else {//any other error is a real problem -> give up
						System.err.println("Task "+task+" "+e.getMessage());
						break;
					}
				}
			}
			try {
				con.close();//always close, also after giving up
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}	
	}
}

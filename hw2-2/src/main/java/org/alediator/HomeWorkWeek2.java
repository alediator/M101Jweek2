package org.alediator;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Hello world!
 *
 */
public class HomeWorkWeek2 
{
	private static String STUDENTS = "students";
	private static String GRADES = "grades";
	
	private static MongoClient client;
	
	
    public static void main( String[] args )
    {
		try {
			client = new MongoClient();
	        DB db = client.getDB(STUDENTS);
	        DBCollection grades = db.getCollection(GRADES);
	        homework22(grades);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
        
    }
    
    /**
     * Remove the documents with the lowest score of any student
     * 
     * @param grades Collection to be managed
     */
    private static void homework22(DBCollection grades){
    	long count = grades.count();
    	if(count == 800){
    		// Remove 200 homework grades with the lowest score
	    	System.out.println(grades.count() + " grades yet...");
	    	Set<Integer> studentsHandled = new HashSet<Integer>();
	    	// Find lowest score
			DBCursor cursor = grades.find(new BasicDBObject("type", "homework")
						.append("student_id", new BasicDBObject("$nin", studentsHandled.toArray())))
					.sort(new BasicDBObject("score", "1")).limit(1);
	    	while (cursor.hasNext()){
	    		DBObject homework = cursor.next();
	    		grades.remove(homework);
	    		studentsHandled.add((Integer) homework.get("student_id"));
	    		cursor.close(); // close cursor
		    	// Find lowest score of any student not handled
	    		cursor = grades.find(new BasicDBObject("type", "homework")
					.append("student_id", new BasicDBObject("$nin", studentsHandled.toArray())))
					.sort(new BasicDBObject("score", "1")).limit(1);
	    	}
	    	cursor.close(); // close the last cursor
	    	// Now must be 600
	    	System.out.println("Now grades size is " + grades.count());
    	}
    }
    
    
}

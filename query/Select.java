package query;

import parser.AST_Select;
import relop.Predicate;
import relop.Schema;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

	 /** Name of tables to select from. */
	  protected String[] tableNames;

	 // Column names to select from.
	  protected String[] columns;
	  
	 // Schema of the table to insert into. 
	  protected Schema schema;

	  // Actual values to insert into the table. 
	  protected Predicate[][] predicates[]; 
	
	
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
	  
	  // Validate table, columns and predicates
	    tableNames = tree.getTables();
	    for (String table : tableNames){
	    	schema = QueryCheck.tableExists(table);
	    	
	    	for (String columnName : columns){
	    		QueryCheck.columnExists(schema, columnName);	
	    	}
	    	for (Predicate[][] predicate : predicates){
	    		QueryCheck.predicates(schema, predicate);
	    	}
	    }
	  
  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Select implements Plan

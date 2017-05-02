package query;

import heap.HeapFile;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

	 // Name of tables to select from. 
	  protected String[] tableNames;

	 // Column names to select from.
	  protected String[] columns;
	  
	 // Schemas of the tables to insert into. 
	  protected Schema[] schemas;
	  
	 // Current schema
	  protected Schema schema;

	 // Predicates to select by
	  protected Predicate[][] predicates; 
	  
	 // Boolean for if we want execute or explain
	  public boolean bool;
	  
	 // The iterator
	  protected Iterator iter;
	
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
	  
	    this.bool = tree.isExplain;
	    this.select(tree);
	    this.select();
	       
  } // public Select(AST_Select tree) throws QueryException

  private void select() {
	  
	this.iter = new FileScan(this.schemas[0], new HeapFile(this.tableNames[0]));
	int n = 1;
	while (n < this.tableNames.length){
		this.iter = new SimpleJoin(this.iter, new FileScan(this.schemas[n], new HeapFile(this.tableNames[n])), new Predicate[0]);
		++n;
	}
	n = 0;
	while (n < this.predicates.length){
		this.iter = new Selection(iter, this.predicates[n]);
		++n;
	}
	if (this.columns.length > 0){
		Integer[] intarr = new Integer[this.columns.length];
		int index = 0;
		while (index < this.columns.length){
			intarr[index] = this.schema.fieldNumber(this.columns[index]);
			++index;
		}
		this.iter = new Projection(this.iter, intarr);
	}
  }

  private void select(AST_Select tree) throws QueryException {
	  
	this.schema = new Schema(0);
	this.tableNames = tree.getTables(); 
	this.schemas = new Schema[this.tableNames.length];
	int n = 0;
	
	while (n < this.tableNames.length){
		this.schemas[n] = QueryCheck.tableExists(tableNames[n]);
		this.schema = Schema.join(schema, this.schemas[n]);
		++n;
	}
	columns = tree.getColumns();
	int index = 0;
	int end = columns.length;
	while (index < end){
		QueryCheck.columnExists(schema, columns[index]);
		++index;
	}
	this.predicates = tree.getPredicates();
	QueryCheck.predicates(schema, this.predicates);
  }

/**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

	  if (this.bool){
		  this.iter.explain(0);
	  } else {
		  int n = this.iter.execute();
		  System.out.print("\n" + n + " rows affected.");
	  }

  } // public void execute()

} // class Select implements Plan

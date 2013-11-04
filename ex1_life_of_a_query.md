Life of a query in SimpleDB
====

Step 1: simpledb.Parser.main() and simpledb.Parser.start()
----

simpledb.Parser.main() is the entry point for the SimpleDB system. It calls simpledb.Parser.start(). The latter performs three main actions:

- It populates the SimpleDB catalog from the catalog text file provided by the user as argument (Database.getCatalog().loadSchema(argv[0]);).

- For each table defined in the system catalog, it computes statistics over the data in the table by calling: TableStats.computeStatistics(), which then does: TableStats s = new TableStats(tableid, IOCOSTPERPAGE);

- It processes the statements submitted by the user (processNextStatement(new ByteArrayInputStream(statementBytes));)

Step 2: simpledb.Parser.processNextStatement()
----

This method takes two key actions:

- First, it gets a physical plan for the query by invoking handleQueryStatement((ZQuery)s);

- Then it executes the query by calling query.execute();

Step 3: simpledb.Parser.handleQueryStatement()
----

This method creates a query object and then takes two key actions:

- It gets a logical plan of the query by parseQueryLogicalPlan(TransactionId tid, ZQuery q);

- Then it gets a physical plan of the query by calling the physicalPlan(TransactionId t, Map<String,TableStats> baseTableStats, boolean explain) method on the logical plan. 

Step 4: simpledb.Parser.parseQueryLogicalPlan(TransactionId tid, ZQuery q)
----

This method takes two key actions:

- It creates a logical plan object.

- It fills in the logical plan object with each parts from Zql query parser.

Step 5: simpledb.LogicalPlan.physicalPlan(TransactionId t, Map<String,TableStats> baseTableStats, boolean explain)
----

This method build the physical plan tree in bottom up order:

- It first build the seq scan node.

- Then it add the filter node on top of the seq scan node.

- Then it creates an JoinOptimizer and uses the optimizer's orderJoins(HashMap<String, TableStats> stats, HashMap<String, Double> filterSelectivities, boolean explain) method to create a optimized join node.

- Then it will add aggregate node and order node if needed.

- Finally, it will add the project node on top and return the project node/iterator.

_Step 6: simpledb.JoinOptimizer.orderJoins(HashMap<String, TableStats> stats, HashMap<String, Double> filterSelectivities, boolean explain)_
----

_We will implement this method later._

Step 7: simpledb.Query.execute()
----

This method will perform two major actions:

- It gets the tupledesc of output and print it out as the first line.

- It keeps calling next out the iterator and print out the resulted tup until the iterator ends.

Query Finished.
---- 


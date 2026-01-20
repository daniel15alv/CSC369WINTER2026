DuckDB pros:
very fast (as long as it is converted into a parquet file)
low memory because program is executed in chunks internally
DuckDB cons:
working with a csv or gzip can be very tricky, espescially with double quotes
can be tricky switching between Python and SQL code

Polars pros:
Also very fast
"lazy" execution allows user to run program without having to load everything 
Polars cons:
there is a steep learning curve to the streaming style of code 

Pandas pros:
most user-friendly and straightforward
Pandas cons:
Heavy on memory; proved to be much slower than the other two 

Personal favorite:
My favorite implementation was in DuckDB; this allowed me to write SQL-styled queries when it comes to extracting relevant information, but to perform other tasks, such as calculations and parsing using Python. 
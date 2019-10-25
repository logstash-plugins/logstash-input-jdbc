create DATABASE jdbc_input_db;

\c jdbc_input_db;

CREATE TABLE employee (
   emp_no integer NOT NULL,
   first_name VARCHAR (50) NOT NULL,
   last_name VARCHAR (50) NOT NULL
);

INSERT INTO employee VALUES (1, 'David', 'Blenkinsop');
INSERT INTO employee VALUES (2, 'Mark', 'Guckenheimer');

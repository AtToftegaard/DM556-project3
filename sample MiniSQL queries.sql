CREATE TABLE Emp (name STRING(50), id INTEGER, age INTEGER);
CREATE TABLE Works (eid INTEGER, depid INTEGER);
CREATE TABLE Dept (did INTEGER, budget INTEGER, depname STRING(50));



INSERT INTO Emp VALUES ('Yongluan', 1 , 28);
INSERT INTO Emp VALUES ('Jacob', 2 , 32);
INSERT INTO Emp VALUES ('Claus', 3 , 42);


INSERT INTO Works VALUES (1, 1);
INSERT INTO Works VALUES (1, 2);
INSERT INTO Works VALUES (2, 1);
INSERT INTO Works VALUES (3, 2);



INSERT INTO Dept VALUES (1, 42 , 'IMADA');
INSERT INTO Dept VALUES (2, 2000 , 'ADMINISTRATION');



-------------------------------------------------------------------------------
-- Sample Queries

EXPLAIN SELECT name,depname FROM Emp, Works,Dept WHERE id = eid AND depid = did;

SELECT name,depname FROM Emp, Works,Dept WHERE id = eid AND depid = did;

SELECT name,depname,budget FROM Emp, Works,Dept WHERE id = eid AND depid = did AND budget > 100;

STATS

source queryenv
javac -d $COMPONENT/classes $COMPONENT/src/qp/utils/*.java
javac -d $COMPONENT/classes $COMPONENT/src/qp/parser/*.java 
javac -d $COMPONENT/classes $COMPONENT/src/qp/operators/*.java 
javac -d $COMPONENT/classes $COMPONENT/src/qp/optimizer/*.java 
javac -d $COMPONENT/classes $COMPONENT/testcases/*.java 
javac -d $COMPONENT/classes $COMPONENT/src/QueryMain.java 
java RandomDB Schedule 10000
java RandomDB Certified 10000
java RandomDB Employees 10000
java RandomDB Aircrafts 10000
java RandomDB Flights 10000
java ConvertTxtToTbl Schedule
java ConvertTxtToTbl Certified
java ConvertTxtToTbl Employees
java ConvertTxtToTbl Aircrafts
java ConvertTxtToTbl Flights
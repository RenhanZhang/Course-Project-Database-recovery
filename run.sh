rm -r output/log/*
rm -r output/dbs/*
./main.o testcases/test00
./main.o testcases/test01
./main.o testcases/test02
./main.o testcases/test03
./main.o testcases/test04
./main.o testcases/test05
./main.o testcases/test06
echo "test00"
diff output/log/log00.log correct/logs/log00.log
diff output/dbs/db00.db correct/dbs/db00.db
echo "test01"
diff output/log/log01.log correct/logs/log01.log
diff output/dbs/db01.db correct/dbs/db01.db
echo "test02"
diff output/log/log02.log correct/logs/log02.log
diff output/dbs/db02.db correct/dbs/db02.db
echo "test03"
diff output/log/log03.log correct/logs/log03.log
diff output/dbs/db03.db correct/dbs/db03.db
echo "test04"
diff output/log/log04.log correct/logs/log04.log
diff output/dbs/db04.db correct/dbs/db04.db
echo "test05"
diff output/log/log05.log correct/logs/log05.log
diff output/dbs/db05.db correct/dbs/db05.db


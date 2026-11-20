# adb_2026_task_3
Task 3 of Winter Semester 2026/27 advanced database proseminar.

Configure your local `src/main/java/adb/setup/ConnectionFactory.java` with the database connection settings.

Run main target class from the project root:

```bash
mvn package
java -jar target/adb-1.0-SNAPSHOT.jar
```

If necessary change the main class in pom.xml:
<mainClass>adb.RestartBlocking</mainClass>

Run secondary targets e.g. Blocking.java from the project root:

```bash
mvn package
java -cp target/adb-1.0-SNAPSHOT.jar adb.Blocking
```

All three classes are runnable either as a standalone or with the benchmark class. 


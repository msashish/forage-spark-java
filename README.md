## Initial setup in POM.xml
    Add maven.compiler properties
    Add Spark-core dependency


## To test by running a class (usual parm passed as -Dexec.args=arg1,arg2...)

    mvn compile exec:java -Dexec.mainClass=ForageTasks
    mvn compile exec:java -Dexec.mainClass=ForageTasks -Dexec.args=-f='/Users/sheelava/msashishgit/forage-java/input/ANZ_synthesised_transaction_dataset.xlsx'

## To test Spark application
    jdk 1.8.0_212
    mvn install or mvn package
    spark-submit --class ForageTasks target/forage-java-tasks-1.0-SNAPSHOT-jar-with-dependencies.jar -f='/Users/sheelava/msashishgit/forage-java/input/ANZ_synthesised_transaction_dataset.xlsx' > output.log
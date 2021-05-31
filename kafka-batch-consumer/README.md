
# Kafka Batch Consumer

Set Java version:

`set PATH=C:\Users\Leon\Downloads\jdk-11\bin;%PATH%`

Maybe you also need to do `set JAVA_HOME=C:\Users\Leon\Downloads\jdk-11`

## Tests

### testAddingElementThenCrashing

Adds a message into Kafka, starts processing then crashes.

`gradlew test --tests *testAddingElementThenCrashing*`

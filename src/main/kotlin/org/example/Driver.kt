package org.example

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient
import software.amazon.awssdk.enhanced.dynamodb.Expression
import software.amazon.awssdk.enhanced.dynamodb.Key
import software.amazon.awssdk.enhanced.dynamodb.TableSchema
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/**
 * This is the entity class that we are trying to possibly concurrent update
 */
@DynamoDbBean
data class MyObject(
    @get:DynamoDbPartitionKey
    var id: String = "...",
    var version: Int = 0,
    var counter: Int = 0,
)

/**
 * This is a demo of a DynamoDB update caller that instead of accepting a
 * new image of MyObject accepts an update function instead
 *
 * the [Updater] calls the supplied update function to derive the new value a given MyObject instance
 * from an existing value (retrieved from the database). Upon committing the change to database,
 * if we notice the version have changed - then we fail the request and calls the supplied function again (passing in the latest
 * instance) - we do this repeatedly until we win the race
 *
 * This ensures values like counters and accumulators can never be overwritten due to concurrent updates - however it does not
 * guarantee that requests are updated in-order
 */
class Updater(ddb: DynamoDbEnhancedClient) {

    private val table = ddb.table("my-object", TableSchema.fromBean(MyObject::class.java))

    fun update(id: String, updateFn: (MyObject) -> MyObject): MyObject {
        val succeeded = false
        var attempts = 1
        while (!succeeded) {
            try {
                val myObject = table.getItem(Key.builder().partitionValue(id).build())
                val updatedMyObject = updateFn.invoke(myObject).copy(version = myObject.version + 1)
                println("Updating to updatedMyObject=$updatedMyObject")
                return table.updateItem(
                    UpdateItemEnhancedRequest
                        .builder(MyObject::class.java)
                        .conditionExpression(condition(myObject))
                        .item(updatedMyObject)
                        .ignoreNulls(true)
                        .build()
                )
            } catch (e: ConditionalCheckFailedException) {
                System.err.println("Condition check failed for attempt $attempts: ${e.message}")
                attempts++
            }
        }
        TODO()
    }

    private fun condition(myObject: MyObject) = Expression
        .builder()
        .expression("version = :version")
        .expressionValues(mapOf(":version" to versionOf(myObject)))
        .build()

    private fun versionOf(myObject: MyObject) = AttributeValue
        .builder()
        .n(myObject.version.toString())
        .build()

}

/**
 * This is the main method that simulates a concurrent environment
 */
fun main() {
    val pool = Executors.newFixedThreadPool(5)
    for (i in 1..10) {
        pool.execute {
            incrementCounter(id = "some-id")
        }
    }
    pool.awaitTermination(30, TimeUnit.SECONDS)
    exitProcess(0)
}

fun incrementCounter(id: String) {

    val ddbClient = DynamoDbClient.create()
    val dynamoDbEnhancedClient = DynamoDbEnhancedClient
        .builder()
        .dynamoDbClient(ddbClient)
        .build()
    val updater = Updater(dynamoDbEnhancedClient)

    updater.update(id = id) {
        val newCount = it.counter + 1
        println("Incrementing count from ${it.counter} to $newCount")
        it.copy(counter = newCount)
    }

}

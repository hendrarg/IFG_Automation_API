import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import static com.kms.katalon.core.testobject.ObjectRepository.findWindowsObject
import com.kms.katalon.core.checkpoint.Checkpoint as Checkpoint
import com.kms.katalon.core.cucumber.keyword.CucumberBuiltinKeywords as CucumberKW
import com.kms.katalon.core.mobile.keyword.MobileBuiltInKeywords as Mobile
import com.kms.katalon.core.model.FailureHandling as FailureHandling
import com.kms.katalon.core.testcase.TestCase as TestCase
import com.kms.katalon.core.testdata.TestData as TestData
import com.kms.katalon.core.testng.keyword.TestNGBuiltinKeywords as TestNGKW
import com.kms.katalon.core.testobject.TestObject as TestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import com.kms.katalon.core.webui.keyword.WebUiBuiltInKeywords as WebUI
import com.kms.katalon.core.windows.keyword.WindowsBuiltinKeywords as Windows
import internal.GlobalVariable as GlobalVariable
import org.openqa.selenium.Keys as Keys
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import groovy.json.JsonSlurper


@Grab('org.apache.kafka:kafka-clients:3.3.1')
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.time.Duration
import java.util.Properties
import groovy.json.JsonSlurper

@Grab('org.apache.kafka:kafka-clients:3.3.1')
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.time.Duration
import java.util.Properties
import groovy.json.JsonSlurper

@Grab('org.apache.kafka:kafka-clients:3.3.1')
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import groovy.json.JsonSlurper

Properties props = new Properties()
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 'localhost:9092')
props.put(ConsumerConfig.GROUP_ID_CONFIG, 'test-group-' + System.currentTimeMillis())

props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)

props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')

def consumer = new KafkaConsumer(props)
consumer.subscribe(['test-messages'])

def records = consumer.poll(Duration.ofSeconds(10))

assert records.count() > 0 : 'No messages received'
println("✅ Received ${records.count()} messages")

records.each { record ->
	def message = record.value()
	println("📨 Message: $message")
	def msgData = new JsonSlurper().parseText(message)
	assert msgData.id != null
	println("✅ Message valid: ID=${msgData.id}")
}

consumer.close()
package kafka

import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import static com.kms.katalon.core.testobject.ObjectRepository.findWindowsObject

import com.kms.katalon.core.annotation.Keyword
import com.kms.katalon.core.checkpoint.Checkpoint
import com.kms.katalon.core.cucumber.keyword.CucumberBuiltinKeywords as CucumberKW
import com.kms.katalon.core.mobile.keyword.MobileBuiltInKeywords as Mobile
import com.kms.katalon.core.model.FailureHandling
import com.kms.katalon.core.testcase.TestCase
import com.kms.katalon.core.testdata.TestData
import com.kms.katalon.core.testobject.TestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import com.kms.katalon.core.webui.keyword.WebUiBuiltInKeywords as WebUI
import com.kms.katalon.core.windows.keyword.WindowsBuiltinKeywords as Windows

import internal.GlobalVariable

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.time.Duration
import java.util.Properties

class KafkaHelper {

	static def createConsumer() {
		Properties props = new Properties()
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 'localhost:9092')
		props.put(ConsumerConfig.GROUP_ID_CONFIG, 'test-group-' + System.currentTimeMillis())
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				'org.apache.kafka.common.serialization.StringDeserializer')
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				'org.apache.kafka.common.serialization.StringDeserializer')
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')

		return new KafkaConsumer(props)
	}

	static def consumeMessages(String topic, int timeoutSeconds = 10) {
		def consumer = createConsumer()
		consumer.subscribe([topic])

		def records = consumer.poll(Duration.ofSeconds(timeoutSeconds))
		consumer.close()

		return records
	}
}

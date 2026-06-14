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

import kafka.KafkaHelper
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import groovy.json.JsonSlurper

import kafka.KafkaHelper
import groovy.json.JsonSlurper

// Consume messages dari topic "test-messages"
def records = KafkaHelper.consumeMessages('test-messages', 10)

// Verify messages received
assert records.count() > 0 : 'No messages received from Kafka'

println("✅ Received ${records.count()} messages from Kafka")

// Validate message content
records.each { record ->
    def message = record.value()
    println("📨 Message: $message")
    
    def jsonSlurper = new JsonSlurper()
    def msgData = jsonSlurper.parseText(message)
    
    // Assertions
    assert msgData.id != null : 'Missing id'
    assert msgData.name != null : 'Missing name'
    assert msgData.email != null : 'Missing email'
    
}
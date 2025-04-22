#include <WiFiClientSecure.h>
#include <MQTT.h>
#include <time.h>

#define RX_PIN 16
#define TX_PIN 17
HardwareSerial SerialSTM32(2);

const char ssid[] = "TT_1558";
const char pass[] = "mvtddmy7a7";

WiFiClientSecure net;
MQTTClient client;
unsigned long lastMillis = 0;

void connect()
{
  Serial.print("Checking WiFi...");
  while (WiFi.status() != WL_CONNECTED)
  {
    Serial.print(".");
    delay(1000);
  }

  Serial.print("\nConnecting to MQTT...");
  net.setInsecure();
  while (!client.connect("testclient", "testclient", "YASSINE2002@**v"))
  {
    Serial.print(".");
    delay(1000);
  }

  Serial.println("\nConnected to MQTT!");
}

void messageReceived(String &topic, String &payload)
{
  Serial.println("Incoming: " + topic + " - " + payload);
}

void setup()
{

  Serial.begin(115200);
  SerialSTM32.begin(9600, SERIAL_8N1, RX_PIN, TX_PIN);

  WiFi.begin(ssid, pass);

  client.begin("ee02914a2862435fa00cf922db4a7465.s1.eu.hivemq.cloud", 8883, net);
  client.onMessage(messageReceived);

  connect();

}

void parseAndPublishCommand(String command)
{

  command.trim();

  int spaceIndex = command.indexOf(' ');

  if (spaceIndex == -1)
  {
    Serial.println("Invalid command format - no space found");
    return;
  }

  String topic = command.substring(0, spaceIndex);
  String value = command.substring(spaceIndex + 1);

  topic.toLowerCase();

  if (topic == "STOCKAGE")
  {
    client.publish("topic_storage_level", value, 0, 1);
    Serial.println("Published to topic_storage_level: " + value);
  }

  else
  {
    Serial.println("Unknown command: " + topic);
  }
}

void loop()
{

  client.loop();

  if (!client.connected())
  {
    connect();
  }

  if (SerialSTM32.available())
  {
    String message = SerialSTM32.readStringUntil('\n');
    message.trim();

    if (message.length() > 0)
    {
      Serial.print("Received from STM32: ");
      Serial.println(message);

      parseAndPublishCommand(message);
    }
  }

  delay(10);
}
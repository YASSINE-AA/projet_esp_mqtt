// This example uses an ESP32 Development Board
// to connect to shiftr.io.
//
// You can check on your device after a successful
// connection here: https://www.shiftr.io/try.
//
// by Joël Gähwiler
// https://github.com/256dpi/arduino-mqtt

#include <WiFiClientSecure.h>
#include <MQTT.h>
#include <time.h>

const char ssid[] = "TT_1558";
const char pass[] = "mvtddmy7a7";

WiFiClientSecure net;
MQTTClient client;

unsigned long lastMillis = 0;

void connect() {
  Serial.print("checking wifi...");
  while (WiFi.status() != WL_CONNECTED) {
    Serial.print(".");
    delay(1000);
  }

  Serial.print("\nconnecting...");
  // do not verify tls certificate
  // check the following example for methods to verify the server:
  // https://github.com/espressif/arduino-esp32/blob/master/libraries/WiFiClientSecure/examples/WiFiClientSecure/WiFiClientSecure.ino
  net.setInsecure();
  while (!client.connect("testclient", "testclient", "YASSINE2002@**v")) {
    Serial.print(".");
    delay(1000);
  }

  

  Serial.println("\nconnected!");

  //client.subscribe("/hello");
  // client.unsubscribe("/hello");
}

void messageReceived(String &topic, String &payload) {
  Serial.println("incoming: " + topic + " - " + payload);

  // Note: Do not use the client in the callback to publish, subscribe or
  // unsubscribe as it may cause deadlocks when other things arrive while
  // sending and receiving acknowledgments. Instead, change a global variable,
  // or push to a queue and handle it in the loop after calling `client.loop()`.
}

void setup() {
  Serial.begin(115200);
  WiFi.begin(ssid, pass);

  // Note: Local domain names (e.g. "Computer.local" on OSX) are not supported
  // by Arduino. You need to set the IP address directly.
  //
  // MQTT brokers usually use port 8883 for secure connections.
  client.begin("ee02914a2862435fa00cf922db4a7465.s1.eu.hivemq.cloud", 8883, net);

  client.onMessage(messageReceived);
  
  
  connect();
}

void loop() {
  client.loop();
  delay(10);  // <- fixes some issues with WiFi stability
  srand(time(NULL));
  int random_number = rand() % 100 + 1;

  if (!client.connected()) {
    connect();
  }

  char test[10];
  snprintf(test, sizeof(test), "%d", random_number);

  // publish a message roughly every second.
  if (millis() - lastMillis > 3000) {
    lastMillis = millis();
    client.publish("topic_storage_level", test, 0, 1);
  }
}
/*
 * Copyright (c) 2025 Yassine Ahmed Ali
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

#include <Gooey/gooey.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <GLPS/glps_audio_stream.h>
#include <MQTTClient.h>
GooeyLabel *login_error_label;
/* Constants */
#define ADDRESS "ssl://cf40b80b591347158d36698519a99fc5.s2.eu.hivemq.cloud:8883"
#define CLIENTID "dashboard"
#define TOPIC_LIGHT "topic_light"
#define TOPIC_STORAGE_LEVEL "topic_storage_level"
#define TOPIC_LIGHT_LEVEL "topic_light_level"
#define QOS 1
#define TIMEOUT 10000L

/* Global Variables */
GooeyWindow *dashboard;
GooeyMeter *light_meter, *storage_meter;
GooeyPlot *light_plot;
GooeyButton *toggle_light, *theme_toggle;
GooeyList *alert_list;
GooeyTheme *dark_theme;
GooeyCanvas *canvas, *login_canvas;
GooeyTabs *tabs;
GooeyImage *login_image, *login_button_icon;
GooeyTextbox *username_textbox, *password_textbox;
GooeyLabel *login_label;
glps_audio_stream *stream;

bool dark_mode = false;
bool light_on = false;
bool mqtt_running = true;

MQTTClient client;
MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
MQTTClient_SSLOptions ssl_opts = MQTTClient_SSLOptions_initializer;
volatile MQTTClient_deliveryToken deliveredtoken;
gthread_t thread_mqtt;

/* Data */
float hours[24] = {
    0, 0, 0, 0, 0,
    10, 30, 70, 80, 85,
    90, 95, 100, 100, 100,
    90, 80, 70, 60, 50,
    30, 10, 0, 0};

float light_levels[24] = {0};

GooeyPlotData plot_data = {
    .x_data = hours,
    .y_data = light_levels,
    .data_count = 5,
    .x_step = 1.0f,
    .y_step = 1.0f,
    .title = "24h Light Levels"};

/* Function Prototypes */
void toggle_light_callback();
void toggle_dark_mode();
void play_audio();
void pause_audio();
void nav_overview();
void nav_system();
void nav_alerts();
void hide_login();
void create_dashboard();
void create_login();
void initialize_dashboard();
void light_slider_callback(long slider_value);
int setup_mqtt_connection(const char *username, const char *password);
void subscribe_to_topics();
void *mqtt_subscribe_thread(void *arg);
void mqtt_cleanup();
void delivered(void *context, MQTTClient_deliveryToken dt);
int msgarrvd(void *context, char *topicName, int topicLen, MQTTClient_message *message);
void connlost(void *context, char *cause);

/* Callback Implementations */
void toggle_light_callback()
{
    light_on = !light_on;
    if (alert_list)
    {
        GooeyList_UpdateItem(alert_list, 0, "Light Status", light_on ? "ON" : "OFF");
    }
}

void toggle_dark_mode()
{
    dark_mode = !dark_mode;
    GooeyWindow_SetTheme(dashboard, dark_mode ? dark_theme : NULL);
    GooeyButton_SetText(theme_toggle, dark_mode ? "Dark Mode ON" : "Dark Mode OFF");
}

void play_audio()
{
    printf("called play \n");
    glps_audio_stream_resume(stream);
}

void pause_audio()
{
    glps_audio_stream_pause(stream);
}

void nav_overview()
{
    GooeyTabs_SetActiveTab(tabs, 0);
}

void nav_system()
{
    GooeyTabs_SetActiveTab(tabs, 1);
}

void nav_alerts()
{
    GooeyTabs_SetActiveTab(tabs, 2);
}

void hide_login()
{
    GooeyWidget_MakeVisible(login_image, false);
    GooeyWidget_MakeVisible(login_button_icon, false);
    GooeyWidget_MakeVisible(login_canvas, false);
    GooeyWidget_MakeVisible(login_label, false);
    GooeyWidget_MakeVisible(username_textbox, false);
    GooeyWidget_MakeVisible(password_textbox, false);
}

void light_slider_callback(long slider_value)
{
    char payload[64];
    snprintf(payload, sizeof(payload), "LIGHT_LEVEL %ld", slider_value);
    printf("slider value at %ld \n", slider_value);
    MQTTClient_publish(client, TOPIC_LIGHT_LEVEL, strlen(payload), payload, QOS, false, NULL);
}

/* MQTT Functions */
int setup_mqtt_connection(const char *username, const char *password)
{
    int rc;
    const char *uri = ADDRESS;
    printf("Using server at %s\n", uri);

    if ((rc = MQTTClient_create(&client, uri, CLIENTID,
                                MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to create client, return code %d\n", rc);
        return rc;
    }

    if ((rc = MQTTClient_setCallbacks(client, NULL, connlost, msgarrvd, delivered)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to set callbacks, return code %d\n", rc);
        MQTTClient_destroy(&client);
        return rc;
    }

    ssl_opts.enableServerCertAuth = 0;
    ssl_opts.verify = 1;
    ssl_opts.CApath = NULL;
    ssl_opts.keyStore = NULL;
    ssl_opts.trustStore = NULL;
    ssl_opts.privateKey = NULL;
    ssl_opts.privateKeyPassword = NULL;
    ssl_opts.enabledCipherSuites = NULL;

    conn_opts.ssl = &ssl_opts;
    conn_opts.keepAliveInterval = 10;
    conn_opts.cleansession = 1;
    conn_opts.username = username;
    conn_opts.password = password;
    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;

    if ((rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to connect, return code %d\n", rc);
        MQTTClient_destroy(&client);
        return rc;
    }

    return MQTTCLIENT_SUCCESS;
}

void subscribe_to_topics()
{
    int rc;
    char *topics[] = {TOPIC_STORAGE_LEVEL, TOPIC_LIGHT};
    int qos[] = {QOS, QOS};
    int topic_count = sizeof(topics) / sizeof(topics[0]);

    if ((rc = MQTTClient_subscribeMany(client, topic_count, topics, qos)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to subscribe, return code %d\n", rc);
    }
    else
    {
        printf("Successfully subscribed to topics\n");
    }
}

void *mqtt_subscribe_thread(void *arg)
{
    subscribe_to_topics();
    while (mqtt_running)
    {
        sleep(1);
    }
    return NULL;
}

void mqtt_cleanup()
{
    mqtt_running = false;
    if (thread_mqtt)
    {
        glps_thread_join(thread_mqtt, NULL);
    }

    char *topics[] = {TOPIC_STORAGE_LEVEL, TOPIC_LIGHT};
    int topic_count = sizeof(topics) / sizeof(topics[0]);
    MQTTClient_unsubscribeMany(client, topic_count, topics);
    MQTTClient_disconnect(client, 10000);
    MQTTClient_destroy(&client);
}

void delivered(void *context, MQTTClient_deliveryToken dt)
{
    printf("Message with token value %d delivery confirmed\n", dt);
    deliveredtoken = dt;
}

int msgarrvd(void *context, char *topicName, int topicLen, MQTTClient_message *message)
{
    printf("Message arrived\n");
    printf("     topic: %s\n", topicName);
    printf("   message: %.*s\n", message->payloadlen, (char *)message->payload);

    if (strcmp(topicName, TOPIC_STORAGE_LEVEL) == 0)
    {
        long value = strtol((char *)message->payload, NULL, 10);
        printf("%ld \n", (long)((long)100 - ((float)value / 47) * 100));
        if (storage_meter)
        {
            printf("Recieved\n");
            GooeyMeter_Update(storage_meter, (long)((long)100 - ((float)value / 47) * 100));
            char storage_level[20];
            snprintf(storage_level, sizeof(storage_level), "%ld%% full!", value);
            GooeyList_UpdateItem(alert_list, 1, "storage Status", storage_level);
        }
    }
    else if (strcmp(topicName, TOPIC_LIGHT) == 0)
    {
        if (alert_list)
        {
            GooeyList_UpdateItem(alert_list, 0, "Light Status", (char *)message->payload);
        }
    }

    MQTTClient_freeMessage(&message);
    MQTTClient_free(topicName);
    return 1;
}

void connlost(void *context, char *cause)
{
    printf("\nConnection lost\n");
    if (cause)
        printf("     cause: %s\n", cause);
}

/* UI Creation Functions */
void create_dashboard()
{
    const char *username = GooeyTextbox_GetText(username_textbox);
    const char *password = GooeyTextbox_GetText(password_textbox);
    if (setup_mqtt_connection(username, password) != MQTTCLIENT_SUCCESS)
    {
        GooeyLabel_SetText(login_error_label, "Please check credentials.");
        fprintf(stderr, "Failed to initialize MQTT connection\n");
        return;
    }
    GooeyLabel_SetText(login_error_label, "");

    hide_login();

    // Create sidebar
    GooeyCanvas *sidebar = GooeyCanvas_Create(0, 60, 250, 640);
    GooeyCanvas_DrawRectangle(sidebar, 0, 0, 60, 640, dashboard->active_theme->widget_base, true, 0.0f, false, 10.0f);

    GooeyImage *dashboard_icon = GooeyImage_Create("dashboard_icon.png", 18, 80, 24, 24, nav_overview);
    GooeyImage *settings_icon = GooeyImage_Create("settings.png", 18, 130, 24, 24, nav_system);
    GooeyImage *alerts_icon = GooeyImage_Create("alerts_icon.png", 18, 180, 24, 24, nav_alerts);
    GooeyImage *help_icon = GooeyImage_Create("help_icon.png", 18, 650, 24, 24, NULL);

    GooeyWindow_RegisterWidget(dashboard, sidebar);
    GooeyWindow_RegisterWidget(dashboard, dashboard_icon);
    GooeyWindow_RegisterWidget(dashboard, settings_icon);
    GooeyWindow_RegisterWidget(dashboard, alerts_icon);
    GooeyWindow_RegisterWidget(dashboard, help_icon);

    // Create tabs
    tabs = GooeyTabs_Create(60, 60, 1150, 660);
    GooeyTabs_InsertTab(tabs, "Overview");
    GooeyTabs_InsertTab(tabs, "System");
    GooeyTabs_InsertTab(tabs, "Alerts");
    GooeyWindow_RegisterWidget(dashboard, tabs);

    // Create header labels
    GooeyLabel *dashboard_bc = GooeyLabel_Create("Home /", 0.28f, 77, 65);
    GooeyLabel *dashboard_title = GooeyLabel_Create("Dashboard", 0.6f, 77, 100);
    GooeyLabel *dashboard_desc = GooeyLabel_Create("This is the overview page, where you monitor your sensors.", 0.26f, 77, 125);
    GooeyLabel *made_with_label = GooeyLabel_Create("Made with Gooey UI ToolKit v1.0.1", 0.3f, 945, 680);

    // Create meters
    light_meter = GooeyMeter_Create(80, 160, 240, 240, 80, "Light", "light_icon.png");
    storage_meter = GooeyMeter_Create(415, 160, 240, 240, 30, "Storage", "storage_icon.png");
    GooeyMeter *battery_meter = GooeyMeter_Create(750, 160, 240, 240, 60, "Battery", "battery_icon.png");

    // Create controls
    GooeySlider *light_slider = GooeySlider_Create(100, 520, 500, 0, 100, true, light_slider_callback);
    toggle_light = GooeyButton_Create("Toggle Light", 100, 570, 220, 30, toggle_light_callback);
    GooeyButton *refresh_btn = GooeyButton_Create("Refresh", 380, 570, 220, 30, NULL);

    // Add widgets to overview tab
    GooeyTabs_AddWidget(tabs, 0, light_meter);
    GooeyTabs_AddWidget(tabs, 0, storage_meter);
    GooeyTabs_AddWidget(tabs, 0, battery_meter);
    GooeyTabs_AddWidget(tabs, 0, dashboard_bc);
    GooeyTabs_AddWidget(tabs, 0, dashboard_title);
    GooeyTabs_AddWidget(tabs, 0, dashboard_desc);
    GooeyTabs_AddWidget(tabs, 0, light_slider);
    GooeyTabs_AddWidget(tabs, 0, toggle_light);
    GooeyTabs_AddWidget(tabs, 0, refresh_btn);

    // Register main widgets
    GooeyWindow_RegisterWidget(dashboard, dashboard_bc);
    GooeyWindow_RegisterWidget(dashboard, dashboard_title);
    GooeyWindow_RegisterWidget(dashboard, dashboard_desc);
    GooeyWindow_RegisterWidget(dashboard, made_with_label);
    GooeyWindow_RegisterWidget(dashboard, light_meter);
    GooeyWindow_RegisterWidget(dashboard, storage_meter);
    GooeyWindow_RegisterWidget(dashboard, battery_meter);

    // Create system info labels
    GooeyLabel *uptime_label = GooeyLabel_Create("Uptime: 3h 24m", 0.27f, 80, 80);
    GooeyLabel *battery_label = GooeyLabel_Create("Battery: 60%", 0.27f, 80, 120);
    GooeyLabel *net_label = GooeyLabel_Create("Network: Connected", 0.27f, 80, 160);
    GooeyLabel *data_label = GooeyLabel_Create("Data Usage: 12.4MB", 0.27f, 80, 200);
    GooeyLabel *load_label = GooeyLabel_Create("CPU Load: 75%", 0.27f, 80, 240);
    GooeyLabel *disk_label = GooeyLabel_Create("Disk Usage: 55%", 0.27f, 80, 280);

    // Add to system tab
    GooeyTabs_AddWidget(tabs, 1, uptime_label);
    GooeyTabs_AddWidget(tabs, 1, battery_label);
    GooeyTabs_AddWidget(tabs, 1, net_label);
    GooeyTabs_AddWidget(tabs, 1, data_label);
    GooeyTabs_AddWidget(tabs, 1, load_label);
    GooeyTabs_AddWidget(tabs, 1, disk_label);

    // Register system widgets
    GooeyWindow_RegisterWidget(dashboard, uptime_label);
    GooeyWindow_RegisterWidget(dashboard, battery_label);
    GooeyWindow_RegisterWidget(dashboard, net_label);
    GooeyWindow_RegisterWidget(dashboard, data_label);
    GooeyWindow_RegisterWidget(dashboard, load_label);
    GooeyWindow_RegisterWidget(dashboard, disk_label);

    // Create alerts list
    alert_list = GooeyList_Create(10, 10, 1120, 580, NULL);
    GooeyList_AddItem(alert_list, "Light Status", light_on ? "ON" : "OFF");
    GooeyList_AddItem(alert_list, "Storage Level", "35% full");
    GooeyList_AddItem(alert_list, "Battery", "60%");
    GooeyList_AddItem(alert_list, "Temperature", "25°C");
    GooeyList_AddItem(alert_list, "Humidity", "40%");
    GooeyList_AddItem(alert_list, "Network", "Connected");
    GooeyList_AddItem(alert_list, "System Status", "All normal");

    // Add to alerts tab
    GooeyTabs_AddWidget(tabs, 2, alert_list);
    GooeyWindow_RegisterWidget(dashboard, alert_list);

    // Start MQTT thread
    glps_thread_create(&thread_mqtt, NULL, mqtt_subscribe_thread, NULL);
}

void create_login()
{
    login_canvas = GooeyCanvas_Create(60, 60, 1150, 660);
    GooeyCanvas_DrawRectangle(login_canvas, 375, 90, 300, 400, dashboard->active_theme->widget_base, true, 0.0f, true, 7.0f);

    login_image = GooeyImage_Create("utilisateur.png", 535, 110, 96, 96, NULL);
    login_label = GooeyLabel_Create("Welcome back!", 0.5f, 500, 250);
    username_textbox = GooeyTextBox_Create(450, 300, 268, 40, "Username", NULL);
    password_textbox = GooeyTextBox_Create(450, 375, 268, 40, "Password", NULL);
    login_button_icon = GooeyImage_Create("login_icon.png", 555, 450, 64, 64, create_dashboard);
    login_error_label = GooeyLabel_Create("", 0.26f, 512, 280);
    GooeyLabel_SetColor(login_error_label, 0xFF0000);
    GooeyWindow_RegisterWidget(dashboard, login_error_label);
    GooeyWindow_RegisterWidget(dashboard, login_image);
    GooeyWindow_RegisterWidget(dashboard, login_button_icon);
    GooeyWindow_RegisterWidget(dashboard, login_canvas);
    GooeyWindow_RegisterWidget(dashboard, login_label);
    GooeyWindow_RegisterWidget(dashboard, username_textbox);
    GooeyWindow_RegisterWidget(dashboard, password_textbox);
}

void initialize_dashboard()
{
    srand(time(NULL));

    dark_theme = GooeyTheme_LoadFromFile("dark.json");
    dashboard = GooeyWindow_Create("Smart Factory Dashboard", 1200, 700, true);
    GooeyWindow_SetTheme(dashboard, dark_theme);
    dark_theme->primary = 0x0062c4;

    // Create header
    canvas = GooeyCanvas_Create(0, 0, 1400, 60);
    GooeyCanvas_DrawRectangle(canvas, 0, 0, 1400, 60, dashboard->active_theme->primary, true, 0.0f, false, 10.0f);
    GooeyCanvas_DrawLine(canvas, 0, 60, 1400, 60, dashboard->active_theme->neutral);

    GooeyImage *logo_icon = GooeyImage_Create("logo_icon.png", 18, 17, 24, 24, NULL);
    GooeyLabel *title = GooeyLabel_Create("Smart Factory", 0.4f, 65, 35);
    theme_toggle = GooeyButton_Create("Dark Mode OFF", 1350, 10, 130, 40, toggle_dark_mode);
    GooeyLabel *avatar_message = GooeyLabel_Create("Welcome back, Mouhib", 0.3f, 960, 35);
    GooeyImage *avatar = GooeyImage_Create("utilisateur.png", 1140, 15, 32, 32, NULL);
    GooeyWindow_RegisterWidget(dashboard, avatar_message);
    GooeyWindow_RegisterWidget(dashboard, logo_icon);
    GooeyWindow_RegisterWidget(dashboard, title);
    GooeyWindow_RegisterWidget(dashboard, avatar);
    GooeyWindow_RegisterWidget(dashboard, canvas);

    create_login();
}

int main()
{
    Gooey_Init();
    initialize_dashboard();
    // glps_audio_stream *stream = glps_audio_stream_init("default", 1024 * 2, 44120, 2, 10000, 1024 * 2);
    // glps_audio_stream_play(stream, "test.mp3", 44120, 2, -1, 1024 * 2);

    if (dashboard)
    {
        GooeyWindow_MakeResizable(dashboard, false);
        GooeyWindow_Run(1, dashboard);
    }

    mqtt_cleanup();

    if (dashboard)
    {
        GooeyWindow_Cleanup(1, dashboard);
    }

    return 0;
}
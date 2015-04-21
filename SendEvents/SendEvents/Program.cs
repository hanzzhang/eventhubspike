// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Microsoft.ServiceBus;

namespace SendEvents
{
    class Program
    {
        static void Main(string[] args)
        {
            SendEvents();

            var useEventProcessorHost = false;
            if (useEventProcessorHost)
            {
                RegisterEventProcessor();
            }
            else
            {
                ReceiveEvents();
            }

            Console.WriteLine("Press any key to exit");
            Console.Read();
        }
        static void RegisterEventProcessor()
        {
            string hostName = "MyEventProcessorHost";
            string eventHubName = ConfigurationManager.AppSettings["EventHubName"];
            string eventHubNamespace = ConfigurationManager.AppSettings["EventHubNamespace"];
            string listenPolicyName = ConfigurationManager.AppSettings["ListenPolicyName"];
            string listenPolicyKey = ConfigurationManager.AppSettings["ListenPolicyKey"];
            string storageAccountName = ConfigurationManager.AppSettings["StorageAccountName"];
            string storageAccountKey = ConfigurationManager.AppSettings["StorageAccountKey"];

            string eventHubConnectionStringForListen = string.Format(
                "Endpoint=sb://{0}.servicebus.windows.net/;SharedAccessKeyName={1};SharedAccessKey={2};TransportType=Amqp",
                eventHubNamespace, listenPolicyName, listenPolicyKey);

            string storageConnectionString = string.Format(
                "DefaultEndpointsProtocol=http;AccountName={0};AccountKey={1};",
                storageAccountName, storageAccountKey);

            var host = new EventProcessorHost(hostName,
                                              eventHubName,
                                              EventHubConsumerGroup.DefaultGroupName,
                                              eventHubConnectionStringForListen,
                                              storageConnectionString
                                              );
            host.RegisterEventProcessorAsync<MyEventProcessor>().Wait();
            Console.WriteLine("RegisterEventProcessor Completed");
        }
        static void SendEvents()
        {
            // Get values from configuration
            int numberOfDevices = int.Parse(ConfigurationManager.AppSettings["NumberOfDevices"]);
            string eventHubName = ConfigurationManager.AppSettings["EventHubName"];
            string eventHubNamespace = ConfigurationManager.AppSettings["EventHubNamespace"];
            string sendPolicyName = ConfigurationManager.AppSettings["SendPolicyName"];
            string sendPolicyKey = ConfigurationManager.AppSettings["SendPolicyKey"];

            string eventHubConnectionString = string.Format(
                "Endpoint=sb://{0}.servicebus.windows.net/;SharedAccessKeyName={1};SharedAccessKey={2};TransportType=Amqp",
                eventHubNamespace, sendPolicyName, sendPolicyKey);

            var client = EventHubClient.CreateFromConnectionString(eventHubConnectionString, eventHubName);

            // Configure JSON to serialize properties using camelCase
            JsonConvert.DefaultSettings = () => new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };

            var random = new Random();

            try
            {
                Console.WriteLine("Sending messages to Event Hub {0}", client.Path);
                //while (!Console.KeyAvailable)
                {
                    var tasks = new List<Task>();

                    // One event per device
                    for (int devices = 0; devices < numberOfDevices; devices++)
                    {
                        // Create the event
                        var info = new Event()
                        {
                            Id = devices.ToString(),
                            Lat = -30 + random.Next(75),
                            Lng = -120 + random.Next(70),
                            Time = DateTime.UtcNow.Ticks,
                            Code = (310 + random.Next(20)).ToString()
                        };

                        // Serialize to JSON
                        var serializedString = JsonConvert.SerializeObject(info);

                        // Create the message data
                        var bytes = Encoding.UTF8.GetBytes(serializedString);
                        var eventData = new EventData(bytes)
                        {
                            //PartitionKey = info.Id
                        };
                        eventData.Properties.Add("event-type", "utf8string");

                        // Send the message to Event Hub
                        Console.WriteLine("send:" + serializedString);
                        tasks.Add(client.SendAsync(eventData));
                    }

                    Task.WaitAll(tasks.ToArray());
                }
            }
            catch (Exception exp)
            {
                Console.WriteLine("Error on send: " + exp.Message);
            }
            Console.WriteLine("SendEvents Completed");
            Console.WriteLine();
        }

        static void ReceiveEvents()
        {
            int numberOfPartitions = Convert.ToInt32(ConfigurationManager.AppSettings["numberOfPartitions"]);
            string eventHubName = ConfigurationManager.AppSettings["EventHubName"];
            string eventHubNamespace = ConfigurationManager.AppSettings["EventHubNamespace"];
            string listenPolicyName = ConfigurationManager.AppSettings["ListenPolicyName"];
            string listenPolicyKey = ConfigurationManager.AppSettings["ListenPolicyKey"];

            string eventHubConnectionString = string.Format(
                "Endpoint=sb://{0}.servicebus.windows.net/;SharedAccessKeyName={1};SharedAccessKey={2};TransportType=Amqp",
                eventHubNamespace, listenPolicyName, listenPolicyKey);

            var client = EventHubClient.CreateFromConnectionString(eventHubConnectionString, eventHubName);

            EventHubConsumerGroup group = client.GetDefaultConsumerGroup();
            for (var i = 0; i < numberOfPartitions; i++)
            {
                var partition = i;
                Task t = Task.Run(() =>
                {
                    var receiver = group.CreateReceiver(client.GetRuntimeInformation().PartitionIds[partition]);
                    bool receive = true;
                    string myOffset;
                    while (receive)
                    {
                        var message = receiver.Receive();
                        myOffset = message.Offset;
                        string body = Encoding.UTF8.GetString(message.GetBytes());
                        Console.WriteLine(String.Format("{0}: {1}: {2}", partition, myOffset, body));
                    }
                });
            }
        }
    }
}

using System;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;

namespace MessageReplicator
{
    public class ServiceBusToEventHub
    {
        private readonly ILogger<ServiceBusToEventHub> _logger;

        public ServiceBusToEventHub(ILogger<ServiceBusToEventHub> log)
        {
            _logger = log;
        }

        [FunctionName("ServiceBusToEventHub")]
        public static async Task Run(
            [ServiceBusTrigger(
                "%TopicName%", 
                "replicator-subscription", 
                Connection = "ServiceBusConnection")] ServiceBusReceivedMessage[] messages,
            [EventHub(
                "%EventHubName%", 
                Connection = "EventHubsConnection")] IAsyncCollector<EventData> events)
        {
            foreach (var msg in messages)
            {
                var messageTypeProperty = msg.ApplicationProperties["MessageType"].ToString();
                EventData data = new EventData(msg.Body);
                data.Properties.Add("MessageType", messageTypeProperty);
                await events.AddAsync(data);
            }
        }
    }
}

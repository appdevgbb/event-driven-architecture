using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using OutboxProcessor.Models;

namespace OutboxProcessor
{
    public static class OrderOutboxWorker
    {
        [FunctionName("OrderOutboxWorker")]
        public static async Task Run(
            [CosmosDBTrigger(
                databaseName: "%CosmosDBDatabaseName%",
                collectionName: "%CosmosDBCollectionName%",
                ConnectionStringSetting = "CosmosDBConnectionString",
                CreateLeaseCollectionIfNotExists = true,
                LeaseCollectionName = "%CosmosDBLeaseCollectionName%")] IReadOnlyList<Document> input,
            [CosmosDB(
                databaseName: "%CosmosDBDatabaseName%",
                collectionName: "%CosmosDBCollectionName%",
                ConnectionStringSetting = "CosmosDBConnectionString",
                SqlQuery = "select * from Orders r where r.orderProcessed = false")] IEnumerable<Document> ordersCreated,
            [CosmosDB(
                databaseName: "%CosmosDBDatabaseName%",
                collectionName: "%CosmosDBCollectionName%",
                ConnectionStringSetting = "CosmosDBConnectionString"
            )] DocumentClient client,
            [ServiceBus(
                "%TopicName%",
                Connection = "ServiceBusConnectionString",
                EntityType = ServiceBusEntityType.Topic
            )] IAsyncCollector<ServiceBusMessage> ordersToProcess,
            ILogger log)
        {
            // This function is triggered off the change feed in CosmosDB. When 
            // new items are added to the orders container, it will be invoked 
            // so that the outbox transaction can be completed.

            if (input != null && input.Count > 0)
            {
                log.LogInformation("Documents modified " + input.Count);
                log.LogInformation("First document Id " + input[0].Id);

                // Iterate throught the collection of orders that are ready to be processed
                foreach (var o in ordersCreated)
                {
                    // Deserialize the document into an order object so that the
                    // Order ID can be referenced when setting message properties
                    var order = JsonConvert.DeserializeObject<Order>(o.ToString());

                    // Create a service bus message with the order object
                    var jsonBody = JsonConvert.SerializeObject(order);
                    var byteArray = Encoding.UTF8.GetBytes(jsonBody);
                    var msg = new ServiceBusMessage(byteArray);

                    // Set the message ID and session ID properties
                    // to the order ID
                    msg.MessageId = order.OrderId.ToString();
                    msg.ContentType = "application/json";
                    msg.SessionId = msg.MessageId;
                    msg.ApplicationProperties.Add("MessageType", "OrderCreatedEvent");

                    // Publish the message
                    await ordersToProcess.AddAsync(msg);                    

                    // Update the order processed flag in the container to complete
                    // the outbox transaction
                    o.SetPropertyValue("orderProcessed", true);
                    await client.ReplaceDocumentAsync(o.SelfLink, o);
                }              
            }
        }
    }
}

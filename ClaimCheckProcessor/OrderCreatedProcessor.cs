using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Linq;
using Azure.Core;
using System.Text;
using Azure.Messaging;
using Azure.Messaging.EventGrid.SystemEvents;
using Azure.Storage.Blobs;
using ClaimCheckProcessor.Models;
using Microsoft.Azure.WebJobs.ServiceBus;
using Azure.Messaging.ServiceBus;

namespace ClaimCheckProcessor
{
    public static class OrderCreatedProcessor
    {
        private static BlobServiceClient _blobServiceClient;

        [FunctionName("OrderCreatedProcessor")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, 
                        "post", 
                        "options", 
                        Route = null)] HttpRequest req,
            [ServiceBus(
                "orders",
                Connection = "ServiceBusConnectionString",
                EntityType = ServiceBusEntityType.Topic
            )] IAsyncCollector<ServiceBusMessage> ordersToProcess,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            // The Options method is invoked when an attempt is made to 
            // create an event grid subscription with the cloud event schema.
            // A response is required to grant event grid permission to push
            // notifications and create the subscription.
            if (req.Method == "OPTIONS")
            {
                // Retrieve the request origin
                if (!req.Headers.TryGetValue("WebHook-Request-Origin", out var headerValues))
                    return new BadRequestObjectResult("Not a valid request");

                // Respond with the origin and rate
                var webhookRequestOrigin = headerValues.FirstOrDefault();
                req.HttpContext.Response.Headers.Add("WebHook-Allowed-Rate", "*");
                req.HttpContext.Response.Headers.Add("WebHook-Allowed-Origin", webhookRequestOrigin);

                return new OkResult();
            }

            // Process notification events
            using (var reader = new StreamReader(req.Body))
            { 
                var payload = await reader.ReadToEndAsync();
                var bytes = Encoding.UTF8.GetBytes(payload);
                var cloudEvents = CloudEvent.ParseMany(new BinaryData(bytes));

                foreach (var e in cloudEvents)
                {
                    switch (e.Type)
                    {
                        // If the blob created event is received then 
                        // read the order details from the blob and send
                        // a message to a service bus topic.
                        case "Microsoft.Storage.BlobCreated":
                            
                            // Deserialize the data from the cloud event.                             
                            var data = e.Data.ToObjectFromJson<StorageBlobCreatedEventData>();
                            if (_blobServiceClient == null)
                            {
                                var connectionString = Environment.GetEnvironmentVariable("OrdersConnectionString");
                                _blobServiceClient = new BlobServiceClient(connectionString);
                            }

                            // Extract the name of the container and blob
                            BlobUriBuilder builder = new BlobUriBuilder(new Uri(data.Url));
                            var containerName = builder.BlobContainerName;
                            var blobName = builder.BlobName;

                            // Download the content and deserialize into the order object
                            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                            BlobClient blobClient = containerClient.GetBlobClient(blobName);
                            var content = await blobClient.DownloadContentAsync();
                            var order = JsonConvert.DeserializeObject<Order>(content.Value.Content.ToString());
                            
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

                            break;                        
                    }
                }
            }

            return new OkResult();
        }
    }
}

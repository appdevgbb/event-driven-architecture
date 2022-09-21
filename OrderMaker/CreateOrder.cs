using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using OrderMaker.Models;
using Microsoft.Azure.Cosmos;

namespace OrderMaker
{
    public class CreateOrder
    {
        private readonly CosmosClient _client;
        private readonly Container _container;
        
        public CreateOrder(CosmosClient cosmosClient)
        { 
            _client = cosmosClient;
            
            var databaseId = Environment.GetEnvironmentVariable("CosmosDBDatabaseId");
            var containerId = Environment.GetEnvironmentVariable("CosmosDBContainerId");
            _container = _client.GetContainer(databaseId, containerId);
        }


        [FunctionName("CreateOrder")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,     
            ILogger log)
        {
            // The function will insert two items into a CosmosDB container to support the 
            // outbox pattern. A transactional batch is used to ensure that both items are
            // written successfully or not at all. 

            log.LogInformation("Incoming order request invoked");

            // Read the request body and deserialize it into an
            // order object so that it can be saved to CosmosDB.
            var requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var incomingOrder = JsonConvert.DeserializeObject<Order>(requestBody);

            // Set the ID of the document to the same value as the Order ID
            incomingOrder.Id = incomingOrder.OrderId;

            // Create an order created (outbox) object for the outbox pattern.
            // The ID of this document must be different that the one for
            // the order object. The OrderProcessed property is used to identify
            // orders that have not been published to a mesage bus.
            var orderCreated = new OrderOutbox
            {
                AccountNumber = incomingOrder.AccountNumber,
                OrderId = incomingOrder.OrderId,
                Quantity = incomingOrder.Quantity,
                Id = Guid.NewGuid().ToString(),
                OrderProcessed = false
            };

            // Transactions must share the same partition key and container in Cosmos.
            // Order ID ('/orderId') is used as the partition key.
            PartitionKey partitionKey = new PartitionKey(incomingOrder.OrderId);

            // Create and execute the batch with the two items
            TransactionalBatch batch = _container.CreateTransactionalBatch(partitionKey)
                .CreateItem<Order>(incomingOrder)
                .CreateItem<OrderOutbox>(orderCreated);

            using TransactionalBatchResponse batchResponse = await batch.ExecuteAsync();            
            if (batchResponse.IsSuccessStatusCode)
            {
                Console.WriteLine("Transactional batch succeeded");
                for (var i = 0; i < batchResponse.Count; i++)
                {
                    var result = batchResponse.GetOperationResultAtIndex<dynamic>(i);
                    Console.WriteLine($"Document {i + 1}:");
                    Console.WriteLine(result.Resource);
                }
            }
            else
            {
                Console.WriteLine("Transactional batch failed");
                for (var i = 0; i < batchResponse.Count; i++)
                {
                    var result = batchResponse.GetOperationResultAtIndex<dynamic>(i);
                    Console.WriteLine($"Document {i + 1}: {result.StatusCode}");
                }
            }
            

            return new OkResult();
        }
    }
}

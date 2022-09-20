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

namespace OrderMaker
{
    public static class CreateOrder
    {
        /// <summary>
        /// 
        /// This HTTP triggered function is invoked with an incoming order request.
        ///
        /// After processing the incoming request, CosmosDB output bindings are used
        /// to insert items into two separate containers:
        ///    
        /// 1) An Orders container to persist the details
        /// 2) An OrdersOutbox container to support a transactional outbox pattern
        ///    
        /// References: https://docs.microsoft.com/en-us/azure/architecture/best-practices/transactional-outbox-cosmos          
        ///    
        /// </summary>
        /// <param name="req">Incoming HTTP request</param>
        /// <param name="orders">Output binding to Orders container</param>
        /// <param name="ordersCreated">Output binding to OrdersOutbux container</param>
        /// <param name="log">Logger</param>
        /// <returns></returns>
        [FunctionName("CreateOrder")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [CosmosDB(
                databaseName: "OrdersDatabase",
                collectionName: "Orders",
                ConnectionStringSetting = "CosmosDBConnectionString"
            )] IAsyncCollector<object> orders,
            [CosmosDB(
                databaseName: "OrdersDatabase",
                collectionName: "OrdersOutbox",
                ConnectionStringSetting = "CosmosDBConnectionString"
            )] IAsyncCollector<object> ordersCreated,
            ILogger log)
        {
            log.LogInformation("Incoming order request invoked");

            // Read the request body and deserialize it into an
            // order object so that it can be saved to CosmosDB.
            var requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var incomingOrder = JsonConvert.DeserializeObject<Order>(requestBody);

            // Insert Order item
            await orders.AddAsync(incomingOrder);

            // Initialize the order processed property to false
            // and insert OrderOutbox item
            var orderCreated = new OrderOutbox { 
                AccountNumber = incomingOrder.AccountNumber,
                OrderId = incomingOrder.OrderId,
                Quantity = incomingOrder.Quantity,
                OrderProcessed = false
            };
            await ordersCreated.AddAsync(orderCreated);


            return new OkResult();
        }
    }
}

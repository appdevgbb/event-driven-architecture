using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Storage.Blobs;
using System.Text;
using OrderMaker.Models;

namespace OrderMaker
{
    public static class OrdersToBlob
    {        
        private static BlobContainerClient client;

        /// <summary>
        /// This HTTP triggered function will take an incoming request for an
        /// order and create a file in a blob container for persistence.
        /// </summary>
        /// <param name="req">Incoming order request</param>
        /// <param name="log">Logger</param>
        /// <returns>The name of the file that was created and saved into a blob container</returns>
        [FunctionName("OrdersToBlob")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            // Read the request body and deserialize it into a
            // order created event 
            var requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var data = JsonConvert.DeserializeObject<Order>(requestBody);

            // Create an instance of the blob container client if it does
            // not already exist
            if (client == null)
            {
                var connectionString = Environment.GetEnvironmentVariable("OrdersConnectionString");
                client = new BlobContainerClient(connectionString, "orders");
            }

            // Create the container, if necessary.
            await client.CreateIfNotExistsAsync();

            // Upload the blob with the order ID as the name of the file.
            using var stream = new MemoryStream(Encoding.ASCII.GetBytes(requestBody));
            await client.UploadBlobAsync($"{data.OrderId}.json", stream);

            // Return a successful message with the file name that was uploaded.
            string responseMessage = $"{data.OrderId}.json uploaded.";
            return new OkObjectResult(responseMessage);            
        }
    }
}

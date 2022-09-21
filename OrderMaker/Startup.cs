using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

[assembly: FunctionsStartup(typeof(OrderMaker.Startup))]

namespace OrderMaker
{
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            // Inject the CosmosDB client as a singleton
            var connectionString = Environment.GetEnvironmentVariable("CosmosDBConnectionString");

            var options = new CosmosClientOptions()
            {
                SerializerOptions = new CosmosSerializationOptions()
                {
                    PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase
                }
            };            

            builder.Services.AddSingleton<CosmosClient>(s => new CosmosClient(connectionString, options));
            //builder.Services.AddSingleton<CosmosClient>(s => new CosmosClient(connectionString));
        }
    }
}

using AccountService;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        var fullyQualifiedNamespace = Environment.GetEnvironmentVariable("AZURE_SERVICE_BUS_NAMESPACE");
        if(string.IsNullOrEmpty(fullyQualifiedNamespace))
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Please ensure the environment variable [AZURE_SERVICE_BUS_NAMESPACE] is set!");
            System.Environment.Exit(0);
        }

        var managedIdentityClientId = Environment.GetEnvironmentVariable("MANAGED_IDENTITY_CLIENT_ID");

        var defaultAzureCredential = string.IsNullOrEmpty(managedIdentityClientId) ? new DefaultAzureCredential() : 
                                                                                     new DefaultAzureCredential(new DefaultAzureCredentialOptions { ManagedIdentityClientId = managedIdentityClientId });

        var sbAdminClient = new ServiceBusAdministrationClient(fullyQualifiedNamespace, defaultAzureCredential);
        var sbClient = new ServiceBusClient(fullyQualifiedNamespace, defaultAzureCredential);

        services.AddHostedService<Worker>();
        services.AddSingleton<ServiceBusAdministrationClient>(implementationInstance: sbAdminClient);
        services.AddSingleton<ServiceBusClient>(implementationInstance: sbClient);
    })
    .Build();

await host.RunAsync();

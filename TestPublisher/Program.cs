using System.Text;
using System.Text.Json;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Shared.Models;

internal class Program
{
    private ServiceBusClient _sbClient;
    private ServiceBusSender _queueSender;
    private HttpClient _httpClient = new HttpClient();
    private Random _random = new Random();

    private static async Task Main(string[] args)
    {
        Program p = new Program();
        await p.SendEvents(3000, p.SendViaClaimCheck, p.SendViaOutbox);
    }

    public Program()
    {
        var sbNamespace = Environment.GetEnvironmentVariable("AZURE_SERVICE_BUS_NAMESPACE");
        if (string.IsNullOrEmpty(sbNamespace))
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Please ensure the environment variable [AZURE_SERVICE_BUS_NAMESPACE] is set!");
            Environment.Exit(0);
        }

        var managedIdentityClientId = Environment.GetEnvironmentVariable("MANAGED_IDENTITY_CLIENT_ID");

        var defaultAzureCredential = string.IsNullOrEmpty(managedIdentityClientId) ? new DefaultAzureCredential() : 
                                                                                     new DefaultAzureCredential(new DefaultAzureCredentialOptions { ManagedIdentityClientId = managedIdentityClientId });

        _sbClient = new ServiceBusClient(sbNamespace, defaultAzureCredential);
        _queueSender = _sbClient.CreateSender("order-orchestration");
    }        

    public async Task SendEvents(int millisecondsDelay, params Func<OrderCreatedEvent, Task>[] eventSendingActions)
    {
        if(eventSendingActions.Length == 0)
            return;

        while(true)
        {
            int actionNum = _random.Next(0, eventSendingActions.Length);
            var orderCreatedEvent = CreateEventPayload();
            await eventSendingActions[actionNum].Invoke(orderCreatedEvent);

            await Task.Delay(millisecondsDelay);
        }
    }

    private async Task SendDirectlyToOrchestrationQueue(OrderCreatedEvent orderCreatedEvent)
    {
        await _queueSender.SendMessageAsync(new ServiceBusMessage
        {
            MessageId = orderCreatedEvent.OrderId.ToString(),
            SessionId = Guid.NewGuid().ToString(),
            ApplicationProperties = { { "MessageType", nameof(OrderCreatedEvent) } },
            Body = BinaryData.FromObjectAsJson(orderCreatedEvent)
        });

        LogSendEvent("Orchestration Queue", orderCreatedEvent);
    }

    private async Task SendViaClaimCheck(OrderCreatedEvent orderCreatedEvent)
    {
        var response = await _httpClient.PostAsync("https://ordermaker.azurewebsites.net/api/OrdersToBlob", 
                                                    new StringContent(JsonSerializer.Serialize<OrderCreatedEvent>(orderCreatedEvent), Encoding.UTF8, "application/json"));

        response.EnsureSuccessStatusCode();
        LogSendEvent("Claim Check Pattern", orderCreatedEvent);
    }

    private async Task SendViaOutbox(OrderCreatedEvent orderCreatedEvent)
    {
        var response = await _httpClient.PostAsync("https://ordermaker.azurewebsites.net/api/CreateOrder", 
                                                    new StringContent(JsonSerializer.Serialize<OrderCreatedEvent>(orderCreatedEvent), Encoding.UTF8, "application/json"));

        response.EnsureSuccessStatusCode();
        LogSendEvent("Outbox Pattern", orderCreatedEvent);
    }

    private OrderCreatedEvent CreateEventPayload()
    {
        return new OrderCreatedEvent
        {
            AccountNumber = _random.Next(1, 7),
            OrderId = Guid.NewGuid(),
            Quantity = _random.Next(1, 101),
        };
    }

    private void LogSendEvent(string sendMethod, OrderCreatedEvent orderCreatedEvent)
    {
        Console.WriteLine($"Event sent via {sendMethod}. OrderId: {orderCreatedEvent.OrderId}, AccountNumber: {orderCreatedEvent.AccountNumber}, Quantity: {orderCreatedEvent.Quantity}");
    }
}